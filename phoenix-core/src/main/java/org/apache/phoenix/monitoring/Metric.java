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
 * Interface that represents phoenix-internal metric.
 */
public interface Metric {
    /**
     * @return Name of the metric
     */
    public String getName();

    /**
     * @return Description of the metric
     */
    public String getDescription();

    /**
     * @return Current value of the metric
     */
    public long getValue();

    /**
     * Change the metric by the specified amount
     * 
     * @param delta
     *            amount by which the metric value should be changed
     */
    public void change(long delta);

    /**
     * Increase the value of metric by 1
     */
    public void increment();
    
    /**
     * Decrease the value of metric by 1
     */
    public void decrement();
    
    /**
     * @return String that represents the current state of the metric. Typically used for logging or reporting purposes.
     */
    public String getCurrentMetricState();
    
    /**
     * Reset the metric
     */
    public void reset();

}

