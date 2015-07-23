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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.apache.phoenix.query.QueryServicesOptions;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SequenceAllocationTest {

    @Test
    /**
     * Validates that sorting a List of SequenceAllocation instances
     * results in the same sort order as sorting SequenceKey instances.
     */
    public void testSortingSequenceAllocation() {
        
        // Arrange
        SequenceKey sequenceKey1 = new SequenceKey(null, "seqalloc", "sequenceC",QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        SequenceKey sequenceKey2 = new SequenceKey(null, "seqalloc", "sequenceB",QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        SequenceKey sequenceKey3 = new SequenceKey(null, "seqalloc", "sequenceA",QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        List<SequenceKey> sequenceKeys = Lists.newArrayList(sequenceKey1, sequenceKey2, sequenceKey3);
        List<SequenceAllocation> sequenceAllocations = Lists.newArrayList(new SequenceAllocation(sequenceKey2, 1), new SequenceAllocation(sequenceKey1, 1), new SequenceAllocation(sequenceKey3, 1));
        
        // Act
        Collections.sort(sequenceKeys);
        Collections.sort(sequenceAllocations);
        
        // Assert
        int i = 0;
        for (SequenceKey sequenceKey : sequenceKeys) {
            assertEquals(sequenceKey, sequenceAllocations.get(i).getSequenceKey());
            i++;
        }
    }
    
    @Test
    public void testSortingSequenceAllocationPreservesAllocations() {
        
        // Arrange
        SequenceKey sequenceKeyC = new SequenceKey(null, "seqalloc", "sequenceC",QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        SequenceKey sequenceKeyB = new SequenceKey(null, "seqalloc", "sequenceB",QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        SequenceKey sequenceKeyA = new SequenceKey(null, "seqalloc", "sequenceA",QueryServicesOptions.DEFAULT_SEQUENCE_TABLE_SALT_BUCKETS);
        List<SequenceAllocation> sequenceAllocations = Lists.newArrayList(new SequenceAllocation(sequenceKeyB, 15), new SequenceAllocation(sequenceKeyC, 11), new SequenceAllocation(sequenceKeyA, 1000));
        
        // Act
        Collections.sort(sequenceAllocations);
        
        // Assert
        assertEquals("sequenceA",sequenceAllocations.get(0).getSequenceKey().getSequenceName());
        assertEquals(1000,sequenceAllocations.get(0).getNumAllocations());
        assertEquals(15,sequenceAllocations.get(1).getNumAllocations());
        assertEquals(11,sequenceAllocations.get(2).getNumAllocations());
    }
}
