package org.apache.phoenix.hbase.index.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

public class IndexedCell implements ExtendedCell {
    private static final byte[] EMPTY = new byte[0];
    private static final long UNINITIALIZED_TS = -1;

    private static int calcHashCode(ImmutableBytesPtr indexTableName, byte[] row) {
        final int prime = 31;
        int result = 1;
        result = prime * result + indexTableName.hashCode();
        result = prime * result + Arrays.hashCode(row);
        return result;
    }

    private ImmutableBytesPtr indexTableName;
    private boolean batchFinished = false;
    private int hashCode;
    private byte[] row;
    private Mutation mutation;
    private long customTimestamp = UNINITIALIZED_TS;
    private long sequenceId = 0;

    public IndexedCell() {}

    public IndexedCell(IndexedCell other) {
        this(other.indexTableName.copyBytes(), other.getMutation());
    }

    public IndexedCell(byte[] indexTableNameBytes, Mutation m) {
        this.indexTableName = new ImmutableBytesPtr(indexTableNameBytes);
        this.mutation = m;
        this.row = m.getRow();
        this.hashCode = calcHashCode(indexTableName, row);
    }

    public void setCustomTimestamp(long timestamp) {
        if (timestamp == UNINITIALIZED_TS) {
            throw new IllegalArgumentException("Cannot initialize timestamp to the reserved value: "
                + UNINITIALIZED_TS);
        }
        this.customTimestamp = timestamp;
    }

    @Override
    public byte[] getRowArray() {
        return row;
    }

    @Override
    public int getRowOffset() {
        return 0;
    }

    @Override
    public short getRowLength() {
        return (short) row.length;
    }

    @Override
    public byte[] getFamilyArray() {
        return WALEdit.METAFAMILY;
    }

    @Override
    public int getFamilyOffset() {
        return 0;
    }

    @Override
    public byte getFamilyLength() {
        return (byte) WALEdit.METAFAMILY.length;
    }

    @Override
    public byte[] getQualifierArray() {
        return IndexedKeyValue.COLUMN_QUALIFIER;
    }

    @Override
    public int getQualifierOffset() {
        return 0;
    }

    @Override
    public int getQualifierLength() {
        return IndexedKeyValue.COLUMN_QUALIFIER.length;
    }

    @Override
    public long getTimestamp() {
        if (customTimestamp != UNINITIALIZED_TS) {
            return customTimestamp;
        }
        return this.mutation.getTimestamp();
    }

    @Override
    public byte[] getValueArray() {
      return EMPTY;
    }

    @Override
    public int getValueOffset() {
        return 0;
    }

    @Override
    public int getValueLength() {
        return 0;
    }

    @Override
    public long getSequenceId() {
        return this.sequenceId;
    }

    @Override
    public byte[] getTagsArray() {
        return EMPTY;
    }

    @Override
    public int getTagsOffset() {
        return 0;
    }

    @Override
    public int getTagsLength() {
        return 0;
    }

    @Override
    public byte getTypeByte() {
        if (mutation instanceof Put) {
            return Type.Put.getCode();
        }
        if (mutation instanceof Delete) {
            return Type.Delete.getCode();
        }
        throw new IllegalArgumentException("Unhandled mutation Type: " + mutation.getClass());
    }

    public byte[] getIndexTable() {
        return this.indexTableName.get();
    }

    public Mutation getMutation() {
        return mutation;
    }

    public boolean getBatchFinished() {
        return this.batchFinished;
    }

    public void markBatchFinished() {
        this.batchFinished = true;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private byte[] getMutationBytes() {
        try {
            MutationProto m = toMutationProto(this.mutation);
            return m.toByteArray();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to get bytes for mutation!", e);
        }
    }
    
    protected MutationProto toMutationProto(Mutation mutation)  throws IOException {
        MutationProto m = null;
        if(mutation instanceof Put){
            m = ProtobufUtil.toMutation(MutationType.PUT, mutation);
        } else if(mutation instanceof Delete) {
            m = ProtobufUtil.toMutation(MutationType.DELETE, mutation);
        } else {
            throw new IOException("Put/Delete mutations only supported");
        }
        return m;
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
        IndexedCell other = (IndexedCell) obj;
        if (hashCode() != other.hashCode()) return false;
        if (!other.indexTableName.equals(this.indexTableName)) return false;
        byte[] current = this.getMutationBytes();
        byte[] otherMutation = other.getMutationBytes();
        if (!Bytes.equals(current, otherMutation)) {
            return false;
        }
        return customTimestamp == other.customTimestamp &&
            sequenceId == other.sequenceId;
    }

    @Override
    public String toString() {
        return "IndexCell:\ttable: " + indexTableName + "\tmutation:" + mutation;
    }

    /**
     * Internal write the underlying data for the entry - this does not do any special prefixing.
     * Writing should be done via {@link KeyValueCodec#write(DataOutput, KeyValue)} to ensure
     * consistent reading/writing of {@link IndexedKeyValue}s.
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
     * This method shouldn't be used - you should use {@link KeyValueCodec#readKeyValue(DataInput)}
     * instead. It's the complement to {@link #writeData(DataOutput)}.
     */
    public void readFields(DataInput in) throws IOException {
        this.indexTableName = new ImmutableBytesPtr(Bytes.readByteArray(in));
        byte[] mutationData = Bytes.readByteArray(in);
        MutationProto mProto = MutationProto.parseFrom(mutationData);
        this.mutation = org.apache.hadoop.hbase.protobuf.ProtobufUtil.toMutation(mProto);
        if (mutation != null) {
            this.row = mutation.getRow();
            this.hashCode = calcHashCode(indexTableName, row);
        }
    }

    @Override
    public long heapSize() {
      // Close?
      return ClassSize.REFERENCE + indexTableName.getLength() +
          Bytes.SIZEOF_BOOLEAN +
          Bytes.SIZEOF_INT +
          ClassSize.sizeOfByteArray(row.length) +
          mutation.heapSize() +
          Bytes.SIZEOF_LONG + Bytes.SIZEOF_LONG;
    }

    @Override
    public void setSequenceId(long seqId) throws IOException {
        this.sequenceId = seqId;
    }

    @Override
    public void setTimestamp(long ts) throws IOException {
        this.customTimestamp = ts;
    }

    @Override
    public void setTimestamp(byte[] ts) throws IOException {
        this.customTimestamp = Bytes.toLong(ts);
    }
}
