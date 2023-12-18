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

import java.io.Closeable;

/**
 * 
 * Memory manager used to track memory usage.  Either throttles
 * memory usage by blocking when the max memory is reached or
 * allocates up to a maximum without blocking.
 * 
 * 
 * @since 0.1
 */
public interface MemoryManager {
    /**
     * Get the total amount of memory (in bytes) that may be allocated.
     */
    long getMaxMemory();
    
    /**
     * Get the amount of available memory (in bytes) not yet allocated.
     */
    long getAvailableMemory();
    
    /**
     * Allocate up to reqBytes of memory, dialing the amount down to 
     * minBytes if full amount is not available.  If minBytes is not
     * available, then this call will block for a configurable amount
     * of time and throw if minBytes does not become available.
     * @param minBytes minimum number of bytes required
     * @param reqBytes requested number of bytes.  Must be greater
     * than or equal to minBytes
     * @return MemoryChunk that was allocated
     * @throws InsufficientMemoryException if unable to allocate minBytes
     *  during configured amount of time
     */
    MemoryChunk allocate(long minBytes, long reqBytes);

    /**
     * Equivalent to calling {@link #allocate(long, long)} where
     * minBytes and reqBytes being the same.
     */
    MemoryChunk allocate(long nBytes);
    
    /**
     * 
     * Chunk of allocated memory.  To reclaim the memory, call {@link #close()}
     *
     * 
     * @since 0.1
     */
    public static interface MemoryChunk extends Closeable {
        /**
         * Get the size in bytes of the allocated chunk.
         */
        long getSize();
        
        /**
         * Free up the memory associated with this chunk
         */
        @Override
        void close();
        
        /**
         * Resize an already allocated memory chunk up or down to a
         * new amount.  If decreasing allocation, this call will not block.
         * If increasing allocation, and nBytes is not available,  then
         * this call will block for a configurable amount of time and
         * throw if nBytes does not become available.  Most commonly
         * used to adjust the allocation of a memory buffer that was
         * originally sized for the worst case scenario.
         * @param nBytes new number of bytes required for this chunk
         * @throws InsufficientMemoryException if unable to allocate minBytes
         *  during configured amount of time
         */
        void resize(long nBytes); 
    }
}
