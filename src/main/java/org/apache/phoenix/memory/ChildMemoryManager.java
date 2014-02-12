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

import org.apache.http.annotation.GuardedBy;
import org.apache.http.annotation.ThreadSafe;

/**
 * 
 * Child memory manager that delegates through to global memory manager,
 * but enforces that at most a threshold percentage is used by this
 * memory manager.  No blocking is done if the threshold is exceeded,
 * but the standard blocking will be done by the global memory manager.
 *
 * 
 * @since 0.1
 */
@ThreadSafe
public class ChildMemoryManager extends DelegatingMemoryManager {
    private final Object sync = new Object();
    private final int maxPercOfTotal;
    @GuardedBy("sync")
    private long allocatedBytes;
    
    public ChildMemoryManager(MemoryManager mm, int maxPercOfTotal) {
        super(mm);
        if (mm instanceof ChildMemoryManager) {
            throw new IllegalStateException("ChildMemoryManager cannot delegate to another ChildMemoryManager");
        }
        this.maxPercOfTotal = maxPercOfTotal;
        if (maxPercOfTotal <= 0 || maxPercOfTotal > 100) {
            throw new IllegalArgumentException("Max percentage of total memory (" + maxPercOfTotal + "%) must be greater than zero and less than or equal to 100");
        }
    }


    private long adjustAllocation(long minBytes, long reqBytes) {
        assert(reqBytes >= minBytes);
        long availBytes = getAvailableMemory();
        // Check if this memory managers percentage of allocated bytes exceeds its allowed maximum
        if (minBytes > availBytes) {
            throw new InsufficientMemoryException("Attempt to allocate more memory than the max allowed of " + maxPercOfTotal + "%");
        }
        // Revise reqBytes down to available memory if necessary
        return Math.min(reqBytes,availBytes);
    }
    
    @Override
    public MemoryChunk allocate(long minBytes, long nBytes) {
        synchronized (sync) {
            nBytes = adjustAllocation(minBytes, nBytes);
            final MemoryChunk chunk = super.allocate(minBytes, nBytes);
            allocatedBytes += chunk.getSize();
            // Instantiate delegate chunk to track allocatedBytes correctly
            return new MemoryChunk() {
                @Override
                public void close() {
                    synchronized (sync) {
                        allocatedBytes -= chunk.getSize();
                        chunk.close();
                    }
                }
    
                @Override
                public long getSize() {
                    return chunk.getSize();
                }
    
                @Override
                public void resize(long nBytes) {
                    synchronized (sync) {
                        long size = getSize();
                        long deltaBytes = nBytes - size;
                        if (deltaBytes > 0) {
                            adjustAllocation(deltaBytes,deltaBytes); // Throw if too much memory
                        }
                        chunk.resize(nBytes);
                        allocatedBytes += deltaBytes;
                    }
                }
            };
        }
    }

    @Override
    public long getAvailableMemory() {
        synchronized (sync) {
            long availBytes = getMaxMemory() - allocatedBytes;
            // Sanity check (should never happen)
            if (availBytes < 0) {
                throw new IllegalStateException("Available memory has become negative: " + availBytes + " bytes.  Allocated memory: " + allocatedBytes + " bytes.");
            }
            return availBytes;
        }
    }
    
    @Override
    public long getMaxMemory() {
        return maxPercOfTotal  * super.getMaxMemory() / 100;
    }
}
