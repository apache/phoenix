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
package org.apache.phoenix.iterate;

import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Interface for scanners that either do aggregation
 * or delegate to scanners that do aggregation.
 *
 * 
 * @since 0.1
 */
public interface AggregatingResultIterator extends ResultIterator {
    /**
     * Provides a means of re-aggregating a result row. For
     * scanners that need to look ahead (i.e. {@link org.apache.phoenix.iterate.OrderedAggregatingResultIterator}
     * @param result the row to re-aggregate
     */
    void aggregate(Tuple result);
}
