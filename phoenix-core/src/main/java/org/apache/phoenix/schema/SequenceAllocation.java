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