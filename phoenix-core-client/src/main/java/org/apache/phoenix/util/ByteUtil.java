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
package org.apache.phoenix.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * Byte utilities
 *
 * 
 * @since 0.1
 */
public class ByteUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteUtil.class);

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final ImmutableBytesPtr EMPTY_BYTE_ARRAY_PTR = new ImmutableBytesPtr(
            EMPTY_BYTE_ARRAY);
    public static final ImmutableBytesWritable EMPTY_IMMUTABLE_BYTE_ARRAY = new ImmutableBytesWritable(
            EMPTY_BYTE_ARRAY);


    /** Mask for bit 0 of a byte. */
    private static final int BIT_0 = 0x01;

    /** Mask for bit 1 of a byte. */
    private static final int BIT_1 = 0x02;

    /** Mask for bit 2 of a byte. */
    private static final int BIT_2 = 0x04;

    /** Mask for bit 3 of a byte. */
    private static final int BIT_3 = 0x08;

    /** Mask for bit 4 of a byte. */
    private static final int BIT_4 = 0x10;

    /** Mask for bit 5 of a byte. */
    private static final int BIT_5 = 0x20;

    /** Mask for bit 6 of a byte. */
    private static final int BIT_6 = 0x40;

    /** Mask for bit 7 of a byte. */
    private static final int BIT_7 = 0x80;

    private static final int[] BITS = {BIT_7, BIT_6, BIT_5, BIT_4, BIT_3, BIT_2, BIT_1, BIT_0};

    public static final byte[] ZERO_BYTE = Bytes.toBytesBinary("\\x00");

    public static final Comparator<ImmutableBytesPtr> BYTES_PTR_COMPARATOR = new Comparator<ImmutableBytesPtr>() {

        @Override
        public int compare(ImmutableBytesPtr o1, ImmutableBytesPtr o2) {
            return Bytes.compareTo(o1.get(), o1.getOffset(), o1.getLength(), o2.get(), o2.getOffset(), o2.getLength());
        }
        
    };

    /**
     * Serialize an array of byte arrays into a single byte array.  Used
     * to pass through a set of bytes arrays as an attribute of a Scan.
     * Use {@link #toByteArrays(byte[], int)} to convert the serialized
     * byte array back to the array of byte arrays.
     * @param byteArrays the array of byte arrays to serialize
     * @return the byte array
     */
    public static byte[] toBytes(byte[][] byteArrays) {
        int size = 0;
        for (byte[] b : byteArrays) {
            if (b == null) {
                size++;
            } else {
                size += b.length;
                size += WritableUtils.getVIntSize(b.length);
            }
        }
        TrustedByteArrayOutputStream bytesOut = new TrustedByteArrayOutputStream(size);
        DataOutputStream out = new DataOutputStream(bytesOut);
        try {
            for (byte[] b : byteArrays) {
                if (b == null) {
                    WritableUtils.writeVInt(out, 0);
                } else {
                    WritableUtils.writeVInt(out, b.length);
                    out.write(b);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
        }
        return bytesOut.getBuffer();
    }

    /**
     * Deserialize a byte array into a set of byte arrays.  Used in
     * coprocessor to reconstruct byte arrays from attribute value
     * passed through the Scan.
     * @param b byte array containing serialized byte arrays (created by {@link #toBytes(byte[][])}).
     * @param length number of byte arrays that were serialized
     * @return array of now deserialized byte arrays
     * @throws IllegalStateException if there are more than length number of byte arrays that were serialized
     */
    public static byte[][] toByteArrays(byte[] b, int length) {
        return toByteArrays(b, 0, length);
    }

    public static byte[][] toByteArrays(byte[] b, int offset, int length) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(b, offset, b.length - offset);
        DataInputStream in = new DataInputStream(bytesIn);
        byte[][] byteArrays = new byte[length][];
        try {
            for (int i = 0; i < length; i++) {
                int bLength = WritableUtils.readVInt(in);
                if (bLength == 0) {
                    byteArrays[i] = null;
                } else {
                    byteArrays[i] = new byte[bLength];
                    int rLength = in.read(byteArrays[i], 0, bLength);
                    assert (rLength == bLength); // For find bugs
                }
            }
            if (in.read() != -1) {
                throw new IllegalStateException("Expected only " + length + " byte arrays, but found more");
            }
            return byteArrays;
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
        }
    }

    public static byte[] serializeVIntArray(int[] intArray) {
        return serializeVIntArray(intArray,intArray.length);
    }

    public static byte[] serializeVIntArray(int[] intArray, int encodedLength) {
        int size = WritableUtils.getVIntSize(encodedLength);
        for (int i = 0; i < intArray.length; i++) {
            size += WritableUtils.getVIntSize(intArray[i]);
        }
        int offset = 0;
        byte[] out = new byte[size];
        offset += ByteUtil.vintToBytes(out, offset, size);
        for (int i = 0; i < intArray.length; i++) {
            offset += ByteUtil.vintToBytes(out, offset, intArray[i]);
        }
        return out;
    }

    public static void serializeVIntArray(DataOutput output, int[] intArray) throws IOException {
        serializeVIntArray(output, intArray, intArray.length);
    }

    /**
     * Allows additional stuff to be encoded in length
     * @param output
     * @param intArray
     * @param encodedLength
     * @throws IOException
     */
    public static void serializeVIntArray(DataOutput output, int[] intArray, int encodedLength) throws IOException {
        WritableUtils.writeVInt(output, encodedLength);
        for (int i = 0; i < intArray.length; i++) {
            WritableUtils.writeVInt(output, intArray[i]);
        }
    }

    public static long[] readFixedLengthLongArray(DataInput input, int length) throws IOException {
        long[] longArray = new long[length];
        for (int i = 0; i < length; i++) {
            longArray[i] = input.readLong();
        }
        return longArray;
    }

    public static void writeFixedLengthLongArray(DataOutput output, long[] longArray) throws IOException {
        for (int i = 0; i < longArray.length; i++) {
            output.writeLong(longArray[i]);
        }
    }

    /**
     * Deserialize a byte array into a int array.  
     * @param b byte array storing serialized vints
     * @return int array
     */
    public static int[] deserializeVIntArray(byte[] b) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(b);
        DataInputStream in = new DataInputStream(bytesIn);
        try {
            int length = WritableUtils.readVInt(in);
            return deserializeVIntArray(in, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
        }
    }

    public static int[] deserializeVIntArray(DataInput in) throws IOException {
        return deserializeVIntArray(in, WritableUtils.readVInt(in));
    }

    public static int[] deserializeVIntArray(DataInput in, int length) throws IOException {
        int i = 0;
        int[] intArray = new int[length];
        while (i < length) {
            intArray[i++] = WritableUtils.readVInt(in);
        }
        return intArray;
    }

    /**
     * Deserialize a byte array into a int array.  
     * @param b byte array storing serialized vints
     * @param length number of serialized vints
     * @return int array
     */
    public static int[] deserializeVIntArray(byte[] b, int length) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(b);
        DataInputStream in = new DataInputStream(bytesIn);
        try {
            return deserializeVIntArray(in,length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Concatenate together one or more byte arrays
     * @param first first byte array
     * @param rest rest of byte arrays
     * @return newly allocated byte array that is a concatenation of all the byte arrays passed in
     */
    public static byte[] concat(byte[] first, byte[]... rest) {
        int totalLength = first.length;
        for (byte[] array : rest) {
            totalLength += array.length;
        }
        byte[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (byte[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static <T> T[] concat(T[] first, T[]... rest) {
        int totalLength = first.length;
        for (T[] array : rest) {
          totalLength += array.length;
        }
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static byte[] concat(SortOrder sortOrder, ImmutableBytesWritable... writables) {
        Preconditions.checkNotNull(sortOrder);
        int totalLength = 0;
        for (ImmutableBytesWritable writable : writables) {
            totalLength += writable.getLength();
        }
        byte[] result = new byte[totalLength];
        int totalOffset = 0;
        for (ImmutableBytesWritable array : writables) {
            byte[] bytes = array.get();
            int offset = array.getOffset();
            if (sortOrder == SortOrder.DESC) {
                bytes = SortOrder.invert(bytes, offset, new byte[array.getLength()], 0, array.getLength());
                offset = 0;
            }
            System.arraycopy(bytes, offset, result, totalOffset, array.getLength());
            totalOffset += array.getLength();
        }
        return result;
    }

    public static int vintFromBytes(byte[] buffer, int offset) {
        return (int)Bytes.readAsVLong(buffer, offset);
    }

    /**
     * Decode a vint from the buffer pointed at to by ptr and
     * increment the offset of the ptr by the length of the
     * vint.
     * @param ptr a pointer to a byte array buffer
     * @return the decoded vint value as an int
     */
    public static int vintFromBytes(ImmutableBytesWritable ptr) {
        return (int) vlongFromBytes(ptr);
    }

    /**
     * Decode a vint from the buffer pointed at to by ptr and
     * increment the offset of the ptr by the length of the
     * vint.
     * @param ptr a pointer to a byte array buffer
     * @return the decoded vint value as a long
     */
    public static long vlongFromBytes(ImmutableBytesWritable ptr) {
        final byte [] buffer = ptr.get();
        final int offset = ptr.getOffset();
        byte firstByte = buffer[offset];
        int len = WritableUtils.decodeVIntSize(firstByte);
        if (len == 1) {
            ptr.set(buffer, offset+1, ptr.getLength());
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len-1; idx++) {
            byte b = buffer[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        ptr.set(buffer, offset+len, ptr.getLength());
        return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
    }

    
    /**
     * Put long as variable length encoded number at the offset in the result byte array
     * @param vint Integer to make a vint of.
     * @param result buffer to put vint into
     * @return Vint length in bytes of vint
     */
    public static int vintToBytes(byte[] result, int offset, final long vint) {
      long i = vint;
      if (i >= -112 && i <= 127) {
        result[offset] = (byte) i;
        return 1;
      }

      int len = -112;
      if (i < 0) {
        i ^= -1L; // take one's complement'
        len = -120;
      }

      long tmp = i;
      while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
      }

      result[offset++] = (byte) len;

      len = (len < -120) ? -(len + 120) : -(len + 112);

      for (int idx = len; idx != 0; idx--) {
        int shiftbits = (idx - 1) * 8;
        long mask = 0xFFL << shiftbits;
        result[offset++] = (byte)((i & mask) >> shiftbits);
      }
      return len + 1;
    }

    /**
     * Increment the key to the next key
     * @param key the key to increment
     * @return a new byte array with the next key or null
     *  if the key could not be incremented because it's
     *  already at its max value.
     */
    public static byte[] nextKey(byte[] key) {
        byte[] nextStartRow = new byte[key.length];
        System.arraycopy(key, 0, nextStartRow, 0, key.length);
        if (!nextKey(nextStartRow, nextStartRow.length)) {
            return null;
        }
        return nextStartRow;
    }

    /**
     * Increment the key in-place to the next key
     * @param key the key to increment
     * @param length the length of the key
     * @return true if the key can be incremented and
     *  false otherwise if the key is at its max
     *  value.
     */
    public static boolean nextKey(byte[] key, int length) {
        return nextKey(key, 0, length);
    }
    
    public static boolean nextKey(byte[] key, int offset, int length) {
        if (length == 0) {
            return false;
        }
        int i = offset + length - 1;
        while (key[i] == -1) {
            key[i] = 0;
            i--;
            if (i < offset) {
                // Change bytes back to the way they were
                do {
                    key[++i] = -1;
                } while (i < offset + length - 1);
                return false;
            }
         }
        key[i] = (byte)(key[i] + 1);
        return true;
    }

    public static byte[] previousKey(byte[] key) {
        byte[] previousKey = new byte[key.length];
        System.arraycopy(key, 0, previousKey, 0, key.length);
        if (!previousKey(previousKey, previousKey.length)) {
            return null;
        }
        return previousKey;
    }

    /**
     * Best attempt to generate largest rowkey smaller than endKey i.e. largest rowkey in the
     * range of [startKey, endKey). If startKey and endKey are empty, the empty key is returned.
     * This function is used to return valid rowkey for some ungrouped aggregation e.g. while
     * returning count value after scanning the rows. If any error or validation issues (e.g.
     * startKey > endKey) are encountered, null value is returned.
     *
     * @param startKey start rowkey for the range.
     * @param endKey end rowkey for the range.
     * @return best attempt of largest rowkey in the range of [startKey, endKey).
     */
    public static byte[] getLargestPossibleRowKeyInRange(byte[] startKey, byte[] endKey) {
        if (startKey.length == 0 && endKey.length == 0) {
            return HConstants.EMPTY_END_ROW;
        }
        byte[] rowKey;
        try {
            if (startKey.length > 0 && endKey.length > 0) {
                int commonBytesIdx = 0;
                while (commonBytesIdx < startKey.length && commonBytesIdx < endKey.length) {
                    if (startKey[commonBytesIdx] == endKey[commonBytesIdx]) {
                        commonBytesIdx++;
                    } else {
                        break;
                    }
                }
                if (commonBytesIdx == 0) {
                    rowKey = ByteUtil.previousKeyWithLength(ByteUtil.concat(endKey,
                                    new byte[startKey.length + 1]),
                            Math.max(endKey.length, startKey.length) + 1);
                } else {
                    byte[] newStartKey;
                    byte[] newEndKey;
                    if (commonBytesIdx < startKey.length) {
                        newStartKey = new byte[startKey.length - commonBytesIdx];
                        System.arraycopy(startKey, commonBytesIdx, newStartKey, 0,
                                newStartKey.length);
                    } else {
                        newStartKey = startKey;
                    }
                    if (commonBytesIdx < endKey.length) {
                        newEndKey = new byte[endKey.length - commonBytesIdx];
                        System.arraycopy(endKey, commonBytesIdx, newEndKey, 0, newEndKey.length);
                    } else {
                        newEndKey = endKey;
                    }
                    byte[] commonBytes = new byte[commonBytesIdx];
                    System.arraycopy(startKey, 0, commonBytes, 0, commonBytesIdx);
                    byte[] tmpRowKey = ByteUtil.previousKeyWithLength(ByteUtil.concat(newEndKey,
                                    new byte[newStartKey.length + 1]),
                            Math.max(newEndKey.length, newStartKey.length) + 1);
                    // tmpRowKey can be null if newEndKey has only \x00 bytes
                    if (tmpRowKey == null) {
                        tmpRowKey = new byte[newEndKey.length - 1];
                        System.arraycopy(newEndKey, 0, tmpRowKey, 0, tmpRowKey.length);
                        rowKey = ByteUtil.concat(commonBytes, tmpRowKey);
                    } else {
                        rowKey = ByteUtil.concat(commonBytes, tmpRowKey);
                    }
                }
            } else if (endKey.length > 0) {
                rowKey = ByteUtil.previousKeyWithLength(ByteUtil.concat(endKey,
                        new byte[1]), endKey.length + 1);
            } else {
                rowKey = ByteUtil.nextKeyWithLength(ByteUtil.concat(startKey,
                        new byte[1]), startKey.length + 1);
            }
            if (rowKey == null) {
                LOGGER.error("Unexpected result while retrieving rowkey in range ({} , {})",
                        Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey));
                return null;
            }
            if (Bytes.compareTo(startKey, rowKey) >= 0
                    || Bytes.compareTo(rowKey, endKey) >= 0) {
                LOGGER.error("Unexpected result while comparing result rowkey in range "
                                + "({} , {}) , rowKey: {}",
                        Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey),
                        Bytes.toStringBinary(rowKey));
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error while retrieving rowkey in range ({} , {})",
                    Bytes.toStringBinary(startKey), Bytes.toStringBinary(endKey));
            return null;
        }
        return rowKey;
    }

    public static byte[] previousKeyWithLength(byte[] key, int length) {
        Preconditions.checkArgument(key.length >= length, "Key length " + key.length + " is "
                + "less than least expected length " + length);
        byte[] previousKey = new byte[length];
        System.arraycopy(key, 0, previousKey, 0, length);
        if (!previousKey(previousKey, length)) {
            return null;
        }
        return previousKey;
    }

    public static byte[] nextKeyWithLength(byte[] key, int length) {
        Preconditions.checkArgument(key.length >= length, "Key length " + key.length + " is "
                + "less than least expected length " + length);
        byte[] nextStartRow = new byte[length];
        System.arraycopy(key, 0, nextStartRow, 0, length);
        if (!nextKey(nextStartRow, length)) {
            return null;
        }
        return nextStartRow;
    }

    public static boolean previousKey(byte[] key, int length) {
        return previousKey(key, 0, length);
    }
    
    public static boolean previousKey(byte[] key, int offset, int length) {
        if (length == 0) {
            return false;
        }
        int i = offset + length - 1;
        while (key[i] == 0) {
            key[i] = -1;
            i--;
            if (i < offset) {
                // Change bytes back to the way they were
                do {
                    key[++i] = 0;
                } while (i < offset + length - 1);
                return false;
            }
         }
        key[i] = (byte)(key[i] - 1);
        return true;
    }

    /**
     * Expand the key to length bytes using a null byte.
     */
    public static byte[] fillKey(byte[] key, int length) {
        if(key.length > length) {
            throw new IllegalStateException();
        }
        if (key.length == length) {
            return key;
        }
        byte[] newBound = new byte[length];
        System.arraycopy(key, 0, newBound, 0, key.length);
        return newBound;
    }

    /**
     * Expand the key to length bytes using the fillByte to fill the
     * bytes beyond the current key length.
     */
    public static void nullPad(ImmutableBytesWritable ptr, int length) {
        if(ptr.getLength() > length) {
            throw new IllegalStateException();
        }
        if (ptr.getLength() == length) {
            return;
        }
        byte[] newBound = new byte[length];
        System.arraycopy(ptr.get(), ptr.getOffset(), newBound, 0, ptr.getLength());
        ptr.set(newBound);
    }

    /**
     * Get the size in bytes of the UTF-8 encoded CharSequence
     * @param sequence the CharSequence
     */
    public static int getSize(CharSequence sequence) {
        int count = 0;
        for (int i = 0, len = sequence.length(); i < len; i++) {
          char ch = sequence.charAt(i);
          if (ch <= 0x7F) {
            count++;
          } else if (ch <= 0x7FF) {
            count += 2;
          } else if (Character.isHighSurrogate(ch)) {
            count += 4;
            ++i;
          } else {
            count += 3;
          }
        }
        return count;
    }

    public static boolean isInclusive(CompareOperator op) {
        switch (op) {
            case LESS:
            case GREATER:
                return false;
            case EQUAL:
            case NOT_EQUAL:
            case LESS_OR_EQUAL:
            case GREATER_OR_EQUAL:
                return true;
            default:
              throw new RuntimeException("Unknown Compare op " + op.name());
        }
    }
    public static boolean compare(CompareOperator op, int compareResult) {
        switch (op) {
            case LESS:
              return compareResult < 0;
            case LESS_OR_EQUAL:
              return compareResult <= 0;
            case EQUAL:
              return compareResult == 0;
            case NOT_EQUAL:
              return compareResult != 0;
            case GREATER_OR_EQUAL:
              return compareResult >= 0;
            case GREATER:
              return compareResult > 0;
            default:
              throw new RuntimeException("Unknown Compare op " + op.name());
        }
    }

    /**
     * Given an ImmutableBytesWritable, returns the payload part of the argument as an byte array. 
     */
    public static byte[] copyKeyBytesIfNecessary(ImmutableBytesWritable ptr) {
        if (ptr.getOffset() == 0 && ptr.getLength() == ptr.get().length) {
            return ptr.get();
        }
        return ptr.copyBytes();
    }

    public static KeyRange getKeyRange(byte[] key, SortOrder order, CompareOperator op,
            PDataType type) {
        op = order.transform(op);
        switch (op) {
        case EQUAL:
            return type.getKeyRange(key, true, key, true, order);
        case GREATER:
            return type.getKeyRange(key, false, KeyRange.UNBOUND, false, order);
        case GREATER_OR_EQUAL:
            return type.getKeyRange(key, true, KeyRange.UNBOUND, false, order);
        case LESS:
            return type.getKeyRange(KeyRange.UNBOUND, false, key, false, order);
        case LESS_OR_EQUAL:
            return type.getKeyRange(KeyRange.UNBOUND, false, key, true, order);
        default:
            throw new IllegalArgumentException("Unknown operator " + op);
        }
    }

    public static boolean contains(Collection<byte[]> keys, byte[] key) {
        for (byte[] k : keys) {
            if (Arrays.equals(k, key)) { return true; }
        }
        return false;
    }

    public static boolean contains(List<ImmutableBytesPtr> keys, ImmutableBytesPtr key) {
        for (ImmutableBytesPtr k : keys) {
            if (key.compareTo(k) == 0) { return true; }
        }
        return false;
    }

    public static boolean match(Set<byte[]> keys, Set<byte[]> keys2) {
        if (keys == keys2) return true;
        if (keys == null || keys2 == null) return false;

        int size = keys.size();
        if (keys2.size() != size) return false;
        for (byte[] k : keys) {
            if (!contains(keys2, k)) { return false; }
        }
        return true;
    }

    public static byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
        // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
        // Search for the place where the trailing 0xFFs start
        int offset = rowKeyPrefix.length;
        while (offset > 0) {
            if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
                break;
            }
            offset--;
        }
        if (offset == 0) {
            // We got an 0xFFFF... (only FFs) stopRow value which is
            // the last possible prefix before the end of the table.
            // So set it to stop at the 'end of the table'
            return HConstants.EMPTY_END_ROW;
        }
        // Copy the right length of the original
        byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
        // And increment the last one
        newStopRow[newStopRow.length - 1]++;
        return newStopRow;
    }

    public static byte[][] splitArrayBySeparator(byte[] src, byte separator){
        List<Integer> separatorLocations = new ArrayList<Integer>();
        for (int k = 0; k < src.length; k++){
            if (src[k] == separator){
                separatorLocations.add(k);
            }
        }
        byte[][] dst = new byte[separatorLocations.size() +1][];
        int previousSepartor = -1;
        for (int j = 0; j < separatorLocations.size(); j++){
            int separatorLocation = separatorLocations.get(j);
            dst[j] = Bytes.copy(src, previousSepartor +1, separatorLocation- previousSepartor -1);
            previousSepartor = separatorLocation;
        }
        if (previousSepartor < src.length){
            dst[separatorLocations.size()] = Bytes.copy(src,
                previousSepartor +1, src.length - previousSepartor -1);
        }
        return dst;
    }

    // Adapted from the Commons Codec BinaryCodec, but treat the input as a byte sequence, without
    // the endinanness reversion in the original code
    public static byte[] fromAscii(final char[] ascii) {
        if (ascii == null || ascii.length == 0) {
            return EMPTY_BYTE_ARRAY;
        }
        final int asciiLength = ascii.length;
        // get length/8 times bytes with 3 bit shifts to the right of the length
        final byte[] l_raw = new byte[asciiLength >> 3];
        // We incr index jj by 8 as we go along to not recompute indices using multiplication every
        // time inside the loop.
        for (int ii = 0, jj = 0; ii < l_raw.length; ii++, jj += 8) {
            for (int bits = 0; bits < BITS.length; ++bits) {
                if (ascii[jj + bits] == '1') {
                    l_raw[ii] |= BITS[bits];
                }
            }
        }
        return l_raw;
    }

    /**
     * Create the closest row after the specified row
     */
    public static byte[] closestPossibleRowAfter(byte[] row) {
        return Arrays.copyOf(row, row.length + 1);
    }
}
