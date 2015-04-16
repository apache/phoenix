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

/**
 * 
 * Statistic that keeps track of the sum of long values that 
 * could be used to represent a phoenix metric. For performance 
 * reasons the internal state in this metric is not strictly covariant
 * and hence should only be used for monitoring and debugging purposes. 
 */
class SizeStatistic implements Metric {
    
    private final AtomicLong total = new AtomicLong(0);
    private final AtomicLong numSamples = new AtomicLong(0);
    private final String name;
    private final String description;
    
    public SizeStatistic(String name, String description) {
        this.name = name;
        this.description = description;
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
        total.set(0);
        numSamples.set(0);
    }
    
    @Override
    public String getCurrentMetricState() {
        return "Name:" + description + ", Total: " + total.get() + ", Number of samples: " + numSamples.get();
    }

    @Override
    public long getNumberOfSamples() {
        return numSamples.get();
    }

    @Override
    public long getTotalSum() {
        return total.get();
    }
    
    public long add(long value) {
        // there is a race condition here but what the heck.
        numSamples.incrementAndGet();
        return total.addAndGet(value);
    }

}
