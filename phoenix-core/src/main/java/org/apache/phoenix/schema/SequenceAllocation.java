/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

/**
 * A SequenceKey and the number of slots requested to be allocated for the sequence. 
 * It binds these two together to allow operations such as sorting
 * a Collection of SequenceKeys and at the same time preserving the associated requested
 * number of slots to allocate.
 * 
 * This class delegates hashCode, equals and compareTo to @see{SequenceKey}.
 *
 */
public class SequenceAllocation implements Comparable<SequenceAllocation> {
    
    private final SequenceKey sequenceKey;
    private final long numAllocations;
    
    public SequenceAllocation(SequenceKey sequenceKey, long numAllocations) {
        this.sequenceKey = sequenceKey;
        this.numAllocations = numAllocations;
    }
    
    
    public SequenceKey getSequenceKey() {
        return sequenceKey;
    }


    public long getNumAllocations() {
        return numAllocations;
    }


    @Override
    public int hashCode() {
        return sequenceKey.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        return sequenceKey.equals(obj);
    }
    
    @Override
    public int compareTo(SequenceAllocation that) {
        return sequenceKey.compareTo(that.sequenceKey);
    }
    
}
