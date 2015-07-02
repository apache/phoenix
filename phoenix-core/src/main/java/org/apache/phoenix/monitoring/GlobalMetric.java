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

/**
 * Class that exposes the various internal phoenix metrics collected
 * at the JVM level. Because metrics are dynamic in nature, it is not guaranteed that the
 * state exposed will always be in sync with each other. One should use
 * these metrics primarily for monitoring and debugging purposes. 
 */
public interface GlobalMetric extends Metric {
    
    /**
     * @return Number of samples collected since the last {@link #reset()} call.
     */
    public long getNumberOfSamples();
    
    /**
     * @return Sum of the values of the metric sampled since the last {@link #reset()} call.
     */
    public long getTotalSum();
}
