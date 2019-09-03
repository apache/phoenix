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

package org.apache.phoenix.hbase.index.wal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

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

    public static IndexedKeyValue newIndexedKeyValue(byte[] bs, Mutation m){
        Cell indexWALCell = adaptFirstCellFromMutation(m);
        return new IndexedKeyValue(indexWALCell, bs, m);
    }

    private static Cell adaptFirstCellFromMutation(Mutation m) {
        if (m != null && m.getFamilyCellMap() != null &&
            m.getFamilyCellMap().firstEntry() != null &&
            m.getFamilyCellMap().firstEntry().getValue() != null
            && m.getFamilyCellMap().firstEntry().getValue().get(0) != null) {
            //have to replace the column family with WALEdit.METAFAMILY to make sure
            //that IndexedKeyValues don't get replicated. The superclass KeyValue fields
            //like row, qualifier and value are placeholders to prevent NPEs
            // when using the KeyValue APIs. See PHOENIX-5188 / 5455
            Cell mutationCell = m.getFamilyCellMap().firstEntry().getValue().get(0);
            CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
            return builder.setFamily(WALEdit.METAFAMILY).
                setQualifier(mutationCell.getQualifierArray()).
                setRow(m.getRow()).
                setTimestamp(mutationCell.getTimestamp()).
                setValue(mutationCell.getValueArray()).setType(Cell.Type.Put).build();
        } else {
            throw new IllegalArgumentException("Tried to create an IndexedKeyValue with a " +
                "Mutation with no Cells!");
        }

    }

    //used for deserialization
    public IndexedKeyValue() {}

    private IndexedKeyValue(Cell c, byte[] bs, Mutation mutation){
        super(c);
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