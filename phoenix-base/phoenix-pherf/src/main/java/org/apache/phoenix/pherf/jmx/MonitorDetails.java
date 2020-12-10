/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.jmx;

import org.apache.phoenix.pherf.jmx.monitors.*;

public enum MonitorDetails {
    FREE_MEMORY("org.apache.phoenix.pherf:type=RuntimeFreeMemory", new FreeMemoryMonitor()),
    TOTAL_MEMORY("org.apache.phoenix.pherf:type=RuntimeTotalMemory", new TotalMemoryMonitor()),
    MAX_MEMORY("org.apache.phoenix.pherf:type=RuntimeMaxMemory", new MaxMemoryMonitor()),
    HEAP_MEMORY_USAGE("org.apache.phoenix.pherf:type=HeapMemoryUsage", new HeapMemoryMonitor()),
    NON_HEAP_MEMORY_USAGE("org.apache.phoenix.pherf:type=NonHeapMemoryUsage", new NonHeapMemoryMonitor()),
    OBJECT_PENDING_FINALIZATION("org.apache.phoenix.pherf:type=ObjectPendingFinalizationCount", new ObjectPendingFinalizationCountMonitor()),
    GARBAGE_COLLECTOR_ELAPSED_TIME("org.apache.phoenix.pherf:type=GarbageCollectorElapsedTime", new GarbageCollectorElapsedTimeMonitor()),
    CPU_LOAD_AVERAGE("org.apache.phoenix.pherf:type=CPULoadAverage", new CPULoadAverageMonitor()),
    THREAD_COUNT("org.apache.phoenix.pherf:type=PherfThreads",new ThreadMonitor());

    private final String monitorName;
    private final Monitor monitor;

    private MonitorDetails(String monitorName, Monitor monitor) {
        this.monitorName = monitorName;
        this.monitor = monitor;
    }

    @Override
    public String toString() {
        return monitorName;
    }

    public Monitor getMonitor() {
        return monitor;
    }
}