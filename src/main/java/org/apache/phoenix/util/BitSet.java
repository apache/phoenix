package org.apache.phoenix.util;

import java.io.*;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * BitSet that can be initialized with primitive types, which
 * is only available in Java 7 or above.
 *
 * @author jtaylor
 * @since 2.1.0
 */
public class BitSet {
    public static final int BITS_PER_LONG = 64;
    public static final int BITS_PER_INT = 32;
    public static final int BITS_PER_SHORT = 16;
    public static final int BITS_PER_BYTE = 8;
    private final long[] bits;
    
    public static int getByteSize(int capacity) {
        if (capacity <= BitSet.BITS_PER_BYTE) {
            return Bytes.SIZEOF_BYTE;
        } else if (capacity <= BitSet.BITS_PER_SHORT) {
            return Bytes.SIZEOF_SHORT;
        } else if (capacity <= BitSet.BITS_PER_INT) {
            return Bytes.SIZEOF_INT;
        } else if (capacity <= BitSet.BITS_PER_LONG) {
            return Bytes.SIZEOF_LONG;
        } else {
            int nLongs = (capacity-1) / BitSet.BITS_PER_LONG + 1;
            return nLongs * Bytes.SIZEOF_LONG;
        }
    }

    public static BitSet read(DataInput input, int capacity) throws IOException {
        if (capacity <= BitSet.BITS_PER_BYTE) {
            return fromPrimitive(input.readByte());
        } else if (capacity <= BitSet.BITS_PER_SHORT) {
            return fromPrimitive(input.readShort());
        } else if (capacity <= BitSet.BITS_PER_INT) {
            return fromPrimitive(input.readInt());
        } else if (capacity <= BitSet.BITS_PER_LONG) {
            return fromPrimitive(input.readLong());
        } else {
            int nLongs = (capacity-1) / BitSet.BITS_PER_LONG + 1;
            return fromArray(ByteUtil.readFixedLengthLongArray(input, nLongs));
        }
    }
    
    public static void write(DataOutput output, BitSet bitSet, int capacity) throws IOException {
        if (capacity <= BitSet.BITS_PER_BYTE) {
            output.writeByte((byte)bitSet.bits[0]);
        } else if (capacity <= BitSet.BITS_PER_SHORT) {
            output.writeShort((short)bitSet.bits[0]);
        } else if (capacity <= BitSet.BITS_PER_INT) {
            output.writeInt((int)bitSet.bits[0]);
        } else if (capacity <= BitSet.BITS_PER_LONG) {
            output.writeLong(bitSet.bits[0]);
        } else {
            ByteUtil.writeFixedLengthLongArray(output, bitSet.bits);
        }
    }
    
    public static BitSet fromPrimitive(byte bits) {
        return new BitSet(new long[] { bits });
    }

    public static BitSet fromPrimitive(short bits) {
        return new BitSet(new long[] { bits });
    }

    public static BitSet fromPrimitive(int bits) {
        return new BitSet(new long[] { bits });
    }

    public static BitSet fromPrimitive(long bits) {
        return new BitSet(new long[] { bits });
    }

    public static BitSet fromArray(long[] bits) {
        return new BitSet(bits);
    }

    public static BitSet withCapacity(int maxBits) {
        int size = Math.max(1,(maxBits + BITS_PER_LONG -1) / BITS_PER_LONG);
        return new BitSet(new long[size]);
    }

    public BitSet(long[] bits) {
        this.bits = bits;
    }

    public boolean get(int nBit) {
        int lIndex = nBit / BITS_PER_LONG;
        int bIndex = nBit % BITS_PER_LONG;
        return (bits[lIndex] & (1L << bIndex)) != 0;
    }
    
    public void set(int nBit) {
        int lIndex = nBit / BITS_PER_LONG;
        int bIndex = nBit % BITS_PER_LONG;
        bits[lIndex] |= (1L << bIndex);
    }
}
