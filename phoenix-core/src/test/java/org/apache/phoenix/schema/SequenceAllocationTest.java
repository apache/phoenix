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
