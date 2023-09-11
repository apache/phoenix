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
package org.apache.phoenix.query;

import org.apache.phoenix.memory.ChildMemoryManager;
import org.apache.phoenix.memory.MemoryManager;

/**
 * 
 * Child QueryServices that delegates through to global QueryService.
 * Used to track memory used by each org to allow a max percentage threshold.
 *
 * 
 * @since 0.1
 */
public class ChildQueryServices extends DelegateConnectionQueryServices {
    private final MemoryManager memoryManager;
    private static final int DEFAULT_MAX_ORG_MEMORY_PERC = 30;
    
    public ChildQueryServices(ConnectionQueryServices services) {
        super(services);
        int maxOrgMemPerc = getProps().getInt(MAX_TENANT_MEMORY_PERC_ATTRIB, DEFAULT_MAX_ORG_MEMORY_PERC);
        this.memoryManager = new ChildMemoryManager(services.getMemoryManager(), maxOrgMemPerc);
    }

    @Override
    public MemoryManager getMemoryManager() {
        return memoryManager;
    }
}
