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

/**
 * 
 * Memory manager that delegates through to another memory manager.
 * 
 * 
 * @since 0.1
 */
public class DelegatingMemoryManager implements MemoryManager {
    private final MemoryManager parent;
    
    public DelegatingMemoryManager(MemoryManager globalMemoryManager){
        this.parent = globalMemoryManager;
    }
    
    @Override
    public long getAvailableMemory() {
        return parent.getAvailableMemory();
    }

    @Override
    public long getMaxMemory() {
        return parent.getMaxMemory();
    }

    @Override
    public MemoryChunk allocate(long minBytes, long reqBytes) {
        return parent.allocate(minBytes, reqBytes);
    }


    @Override
    public MemoryChunk allocate(long nBytes) {
        return allocate(nBytes, nBytes);
    }

    public MemoryManager getParent() {
        return parent;
    }

}
