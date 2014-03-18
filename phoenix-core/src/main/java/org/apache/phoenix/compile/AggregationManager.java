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
package org.apache.phoenix.compile;

import org.apache.phoenix.expression.aggregator.ClientAggregators;

/**
 * 
 * Class that manages aggregations during query compilation
 *
 * 
 * @since 0.1
 */
public class AggregationManager {
    private ClientAggregators aggregators;
    private int position = 0;
    
    public AggregationManager() {
    }

    public ClientAggregators getAggregators() {
        return aggregators;
    }
    
    public boolean isEmpty() {
        return aggregators == null || aggregators.getAggregatorCount() == 0;
    }
    
    /**
     * @return allocate the next available zero-based positional index
     * for the client-side aggregate function.
     */
    protected int nextPosition() {
        return position++;
    }
    
    public void setAggregators(ClientAggregators clientAggregator) {
        this.aggregators = clientAggregator;
    }
}
