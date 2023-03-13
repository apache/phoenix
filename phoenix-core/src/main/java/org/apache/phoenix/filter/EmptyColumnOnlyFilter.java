package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.ScanUtil;

/**
 * For the queries that returns only empty column
 */
public class EmptyColumnOnlyFilter extends FilterBase implements Writable {
    private byte[] emptyCF;
    private byte[] emptyCQ;
    private boolean found;

    public EmptyColumnOnlyFilter() {}
    public EmptyColumnOnlyFilter(byte[] emptyCF, byte[] emptyCQ) {
        Preconditions.checkArgument(emptyCF != null,
                "Column family must not be null");
        Preconditions.checkArgument(emptyCQ != null,
                "Column qualifier must not be null");
        this.emptyCF = emptyCF;
        this.emptyCQ = emptyCQ;
    }

    @Override
    public void reset() throws IOException {
        found = false;
    }
    @Deprecated
    @Override
    public ReturnCode filterKeyValue(final Cell c) throws IOException {
        return filterCell(c);
    }

    @Override
    public ReturnCode filterCell(final Cell cell) throws IOException {
        if (found) {
            return ReturnCode.NEXT_ROW;
        }
        if (ScanUtil.isEmptyColumn(cell, emptyCF)) {
            found = true;
            return ReturnCode.INCLUDE;
        }
        return ReturnCode.NEXT_COL;
    }

    public static EmptyColumnOnlyFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            return (EmptyColumnOnlyFilter) Writables.getWritable(pbBytes, new EmptyColumnOnlyFilter());
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(emptyCF.length);
        out.write(emptyCF);
        out.writeInt(emptyCQ.length);
        out.write(emptyCQ);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        emptyCF = new byte[length];
        in.readFully(emptyCF, 0, length);
        length = in.readInt();
        emptyCQ = new byte[length];
        in.readFully(emptyCQ, 0, length);
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }
}