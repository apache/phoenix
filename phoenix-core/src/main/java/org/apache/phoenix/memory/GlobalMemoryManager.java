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
package org.apache.phoenix.memory;

import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MEMORY_MANAGER_BYTES;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.MEMORY_WAIT_TIME;

import org.apache.http.annotation.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * Global memory manager to track course grained memory usage across all requests.
 *
 * 
 * @since 0.1
 */
public class GlobalMemoryManager implements MemoryManager {
    private static final Logger logger = LoggerFactory.getLogger(GlobalMemoryManager.class);
    
    private final Object sync = new Object();
    private final long maxMemoryBytes;
    private final int maxWaitMs;
    @GuardedBy("sync")
    private volatile long usedMemoryBytes;
    public GlobalMemoryManager(long maxBytes, int maxWaitMs) {
        if (maxBytes <= 0) {
            throw new IllegalStateException("Total number of available bytes (" + maxBytes + ") must be greater than zero");
        }
        if (maxWaitMs < 0) {
            throw new IllegalStateException("Maximum wait time (" + maxWaitMs + ") must be greater than or equal to zero");
        }
        this.maxMemoryBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.usedMemoryBytes = 0;
    }
    
    @Override
    public long getAvailableMemory() {
        synchronized(sync) {
            return maxMemoryBytes - usedMemoryBytes;
        }
    }

    @Override
    public long getMaxMemory() {
        return maxMemoryBytes;
    }


    // TODO: Work on fairness: One big memory request can cause all others to block here.
    private long allocateBytes(long minBytes, long reqBytes) {
        if (minBytes < 0 || reqBytes < 0) {
            throw new IllegalStateException("Minimum requested bytes (" + minBytes + ") and requested bytes (" + reqBytes + ") must be greater than zero");
        }
        if (minBytes > maxMemoryBytes) { // No need to wait, since we'll never have this much available
            throw new InsufficientMemoryException("Requested memory of " + minBytes + " bytes is larger than global pool of " + maxMemoryBytes + " bytes.");
        }
        long startTimeMs = System.currentTimeMillis(); // Get time outside of sync block to account for waiting for lock
        long nBytes;
        synchronized(sync) {
            while (usedMemoryBytes + minBytes > maxMemoryBytes) { // Only wait if minBytes not available
                try {
                    long remainingWaitTimeMs = maxWaitMs - (System.currentTimeMillis() - startTimeMs);
                    if (remainingWaitTimeMs <= 0) { // Ran out of time waiting for some memory to get freed up
                        throw new InsufficientMemoryException("Requested memory of " + minBytes + " bytes could not be allocated from remaining memory of " + usedMemoryBytes + " bytes from global pool of " + maxMemoryBytes + " bytes after waiting for " + maxWaitMs + "ms.");
                    }
                    sync.wait(remainingWaitTimeMs);
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Interrupted allocation of " + minBytes + " bytes", ie);
                }
            }
            // Allocate at most reqBytes, but at least minBytes
            nBytes = Math.min(reqBytes, maxMemoryBytes - usedMemoryBytes);
            if (nBytes < minBytes) {
                throw new IllegalStateException("Allocated bytes (" + nBytes + ") should be at least the minimum requested bytes (" + minBytes + ")");
            }
            usedMemoryBytes += nBytes;
        }
        MEMORY_WAIT_TIME.update(System.currentTimeMillis() - startTimeMs);
        MEMORY_MANAGER_BYTES.update(nBytes);
        return nBytes;
    }

    @Override
    public MemoryChunk allocate(long minBytes, long reqBytes) {
        long nBytes = allocateBytes(minBytes, reqBytes);
        return newMemoryChunk(nBytes);
    }

    @Override
    public MemoryChunk allocate(long nBytes) {
        return allocate(nBytes,nBytes);
    }

    protected MemoryChunk newMemoryChunk(long sizeBytes) {
        return new GlobalMemoryChunk(sizeBytes);
    }
    
    private class GlobalMemoryChunk implements MemoryChunk {
        private volatile long size;

        private GlobalMemoryChunk(long size) {
            if (size < 0) {
                throw new IllegalStateException("Size of memory chunk must be greater than zero, but instead is " + size);
            }
            this.size = size;
        }

        @Override
        public long getSize() {
            synchronized(sync) {
                return size; // TODO: does this need to be synchronized?
            }
        }
        
        @Override
        public void resize(long nBytes) {
            if (nBytes < 0) {
                throw new IllegalStateException("Number of bytes to resize to must be greater than zero, but instead is " + nBytes);
            }
            synchronized(sync) {
                long nAdditionalBytes = (nBytes - size);
                if (nAdditionalBytes < 0) {
                    usedMemoryBytes += nAdditionalBytes;
                    size = nBytes;
                    sync.notifyAll();
                } else {
                    allocateBytes(nAdditionalBytes, nAdditionalBytes);
                    size = nBytes;
                }
            }
        }
        
        /**
         * Check that MemoryChunk has previously been closed.
         */
        @Override
        protected void finalize() throws Throwable {
            try {
                if (size > 0) {
                    logger.warn("Orphaned chunk of " + size + " bytes found during finalize");
                }
                close();
                // TODO: log error here, but we can't use SFDC logging
                // because this runs in an hbase coprocessor.
                // Create a gack-like API (talk with GridForce or HBase folks)
            } finally {
                super.finalize();
            }
        }
        
        @Override
        public void close() {
            synchronized(sync) {
                usedMemoryBytes -= size;
                size = 0;
                sync.notifyAll();
            }
        }
    }
}
 
