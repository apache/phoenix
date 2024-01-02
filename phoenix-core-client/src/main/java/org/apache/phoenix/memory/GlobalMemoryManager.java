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

import net.jcip.annotations.GuardedBy;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalMemoryManager.class);

    private final Object sync = new Object();
    private final long maxMemoryBytes;
    @GuardedBy("sync")
    private volatile long usedMemoryBytes;
    public GlobalMemoryManager(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalStateException(
                    "Total number of available bytes (" + maxBytes + ") must be greater than zero");
        }
        this.maxMemoryBytes = maxBytes;
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

    // TODO: Work on fairness: One big memory request can cause all others to fail here.
    private long allocateBytes(long minBytes, long reqBytes) {
        if (minBytes < 0 || reqBytes < 0) {
            throw new IllegalStateException("Minimum requested bytes (" + minBytes
                    + ") and requested bytes (" + reqBytes + ") must be greater than zero");
        }
        if (minBytes > maxMemoryBytes) {
            throw new InsufficientMemoryException(
                    new SQLExceptionInfo.Builder(SQLExceptionCode.INSUFFICIENT_MEMORY)
                    .setMessage("Requested memory of " + minBytes
                              + " bytes is larger than global pool of " + maxMemoryBytes + " bytes.")
                    .build().buildException());
        }
        long nBytes;
        synchronized(sync) {
            if (usedMemoryBytes + minBytes > maxMemoryBytes) {
                throw new InsufficientMemoryException(
                        new SQLExceptionInfo.Builder(SQLExceptionCode.INSUFFICIENT_MEMORY)
                        .setMessage("Requested memory of " + minBytes
                                + " bytes could not be allocated. Using memory of " + usedMemoryBytes
                                + " bytes from global pool of " + maxMemoryBytes)
                        .build().buildException());
            }
            // Allocate at most reqBytes, but at least minBytes
            nBytes = Math.min(reqBytes, maxMemoryBytes - usedMemoryBytes);
            if (nBytes < minBytes) {
                throw new IllegalStateException("Allocated bytes (" + nBytes
                        + ") should be at least the minimum requested bytes (" + minBytes + ")");
            }
            usedMemoryBytes += nBytes;
        }
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

    private MemoryChunk newMemoryChunk(long sizeBytes) {
        return new GlobalMemoryChunk(sizeBytes);
    }

    private class GlobalMemoryChunk implements MemoryChunk {
        private volatile long size;
        //private volatile String stack;

        private GlobalMemoryChunk(long size) {
            if (size < 0) {
                throw new IllegalStateException("Size of memory chunk must be greater than zero, but instead is " + size);
            }
            this.size = size;
            // Useful for debugging where a piece of memory was allocated
            // this.stack = ExceptionUtils.getStackTrace(new Throwable());
        }

        @Override
        public long getSize() {
            return size;
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
                } else {
                    allocateBytes(nAdditionalBytes, nAdditionalBytes);
                    size = nBytes;
                    //this.stack = ExceptionUtils.getStackTrace(new Throwable());
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
                    LOGGER.warn("Orphaned chunk of " + size + " bytes found during finalize");
                    //logger.warn("Orphaned chunk of " + size + " bytes found during finalize allocated here:\n" + stack);
                }
                freeMemory();
            } finally {
                super.finalize();
            }
        }

        private void freeMemory() {
            synchronized(sync) {
                usedMemoryBytes -= size;
                size = 0;
            }
        }
        
        @Override
        public void close() {
            freeMemory();
        }
    }
}

