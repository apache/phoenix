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
package org.apache.phoenix.monitoring;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Incrementing only counter that keeps track of the 
 * number of occurrences of something.
 * 
 */
@ThreadSafe
class Counter implements Metric {
    
    private final AtomicLong counter;
    private final String name;
    private final String description;
    
    public Counter(String name, String description) {
        this.name = name;
        this.description = description;
        this.counter = new AtomicLong(0);
    }
    
    public long increment() {
        return counter.incrementAndGet();
    }
    
    public long getCurrentCount() {
        return counter.get();
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public String getDescription() {
        return description;
    }
    
    @Override
    public void reset() {
        counter.set(0);
    }
    
    @Override
    public String toString() {
        return "Name: " + name + ", Current count: " + counter.get();
    }
    
    @Override
    public String getCurrentMetricState() {
        return toString();
    }

    @Override
    public long getNumberOfSamples() {
        return getCurrentCount();
    }

    @Override
    public long getTotalSum() {
        return getCurrentCount();
    }
    
}
