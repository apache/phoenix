package org.apache.hadoop.hbase.index.wal;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.index.util.ImmutableBytesPtr;

public class IndexedKeyValue extends KeyValue {
    private static int calcHashCode(ImmutableBytesPtr indexTableName, Mutation mutation) {
        final int prime = 31;
        int result = 1;
        result = prime * result + indexTableName.hashCode();
        result = prime * result + Arrays.hashCode(mutation.getRow());
        return result;
    }

    private ImmutableBytesPtr indexTableName;
    private Mutation mutation;
    // optimization check to ensure that batches don't get replayed to the index more than once
    private boolean batchFinished = false;
    private int hashCode;

    public IndexedKeyValue() {}

    public IndexedKeyValue(byte[] bs, Mutation mutation) {
        this.indexTableName = new ImmutableBytesPtr(bs);
        this.mutation = mutation;
        this.hashCode = calcHashCode(indexTableName, mutation);
    }

    public byte[] getIndexTable() {
        return this.indexTableName.get();
    }

    public Mutation getMutation() {
        return mutation;
    }

    /**
     * This is a KeyValue that shouldn't actually be replayed, so we always mark it as an {@link WALEdit#METAFAMILY} so it
     * isn't replayed via the normal replay mechanism
     */
    @Override
    public boolean matchingFamily(final byte[] family) {
        return Bytes.equals(family, WALEdit.METAFAMILY);
    }
    
    /**
     * Not a real KeyValue
     */
    @Override
    public boolean matchingRow(final byte [] row) {
        return false;
    }

    @Override
    public String toString() {
        return "IndexWrite:\n\ttable: " + indexTableName + "\n\tmutation:" + mutation;
    }

    /**
     * This is a very heavy-weight operation and should only be done when absolutely necessary - it does a full
     * serialization of the underyling mutation to compare the underlying data.
     */
    @Override
    public boolean equals(Object obj) {
        if(obj == null) return false;
        if (this == obj) return true;
        if (getClass() != obj.getClass()) return false;
        IndexedKeyValue other = (IndexedKeyValue)obj;
        if (hashCode() != other.hashCode()) return false;
        if (!other.indexTableName.equals(this.indexTableName)) return false;
        byte[] current = this.getMutationBytes();
        byte[] otherMutation = other.getMutationBytes();
        return Bytes.equals(current, otherMutation);
    }

    private byte[] getMutationBytes() {
        try {
            MutationProto m = toMutationProto(this.mutation);
            return m.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to get bytes for mutation!", e);
        }
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * Internal write the underlying data for the entry - this does not do any special prefixing. Writing should be done
     * via {@link KeyValueCodec#write(DataOutput, KeyValue)} to ensure consistent reading/writing of
     * {@link IndexedKeyValue}s.
     * 
     * @param out
     *            to write data to. Does not close or flush the passed object.
     * @throws IOException
     *             if there is a problem writing the underlying data
     */
    void writeData(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, this.indexTableName.get());
        MutationProto m = toMutationProto(this.mutation);
        Bytes.writeByteArray(out, m.toByteArray());
    }

    /**
     * This method shouldn't be used - you should use {@link KeyValueCodec#readKeyValue(DataInput)} instead. Its the
     * complement to {@link #writeData(DataOutput)}.
     */
    @SuppressWarnings("javadoc")
    public void readFields(DataInput in) throws IOException {
        this.indexTableName = new ImmutableBytesPtr(Bytes.readByteArray(in));
        byte[] mutationData = Bytes.readByteArray(in);
        MutationProto mProto = MutationProto.parseFrom(mutationData);
        this.mutation = org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(mProto);
        this.hashCode = calcHashCode(indexTableName, mutation);
    }

    public boolean getBatchFinished() {
        return this.batchFinished;
    }

    public void markBatchFinished() {
        this.batchFinished = true;
    }
    
    protected MutationProto toMutationProto(Mutation mutation)  throws IOException {
        MutationProto m = null;
        if(mutation instanceof Put){
            m = org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(MutationType.PUT, 
                mutation);
        } else if(mutation instanceof Delete) {
            m = org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(MutationType.DELETE, 
                mutation);
        } else {
            throw new IOException("Put/Delete mutations only supported");
        }
        return m;
    }
}