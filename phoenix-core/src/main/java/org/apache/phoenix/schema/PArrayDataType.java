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
package org.apache.phoenix.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

/**
 * The datatype for PColummns that are Arrays. Any variable length array would follow the below order. 
 * Every element would be seperated by a seperator byte '0'. Null elements are counted and once a first 
 * non null element appears we write the count of the nulls prefixed with a seperator byte.
 * Trailing nulls are not taken into account. The last non null element is followed by two seperator bytes. 
 * For eg a, b, null, null, c, null -> 65 0 66 0 0 2 67 0 0 0 
 * a null null null b c null d -> 65 0 0 3 66 0 67 0 0 1 68 0 0 0.
 * The reason we use this serialization format is to allow the
 * byte array of arrays of the same type to be directly comparable against each other. 
 * This prevents a costly deserialization on compare and allows an array column to be used as the last column in a primary key constraint.
 */
public class PArrayDataType {

    public static final byte ARRAY_SERIALIZATION_VERSION = 1;
	public PArrayDataType() {
	}

	public byte[] toBytes(Object object, PDataType baseType) {
		if(object == null) {
			throw new ConstraintViolationException(this + " may not be null");
		}
		Pair<Integer, Integer> nullsVsNullRepeationCounter = new Pair<Integer, Integer>();
        int size = estimateByteSize(object, nullsVsNullRepeationCounter,
                PDataType.fromTypeId((baseType.getSqlType() + Types.ARRAY)));
        int noOfElements = ((PhoenixArray)object).numElements;
        if(noOfElements == 0) {
        	return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        TrustedByteArrayOutputStream byteStream = null;
		if (!baseType.isFixedWidth()) {
		    size += ((2 * Bytes.SIZEOF_BYTE) + (noOfElements - nullsVsNullRepeationCounter.getFirst()) * Bytes.SIZEOF_BYTE)
		                                + (nullsVsNullRepeationCounter.getSecond() * 2 * Bytes.SIZEOF_BYTE);
		    // Assume an offset array that fit into Short.MAX_VALUE.  Also not considering nulls that could be > 255
		    // In both of these cases, finally an array copy would happen
		    int capacity = noOfElements * Bytes.SIZEOF_SHORT;
		    // Here the int for noofelements, byte for the version, int for the offsetarray position and 2 bytes for the end seperator
            byteStream = new TrustedByteArrayOutputStream(size + capacity + Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE +  Bytes.SIZEOF_INT);
		} else {
		    // Here the int for noofelements, byte for the version
		    byteStream = new TrustedByteArrayOutputStream(size + Bytes.SIZEOF_INT+ Bytes.SIZEOF_BYTE);
		}
		DataOutputStream oStream = new DataOutputStream(byteStream);
		return createArrayBytes(byteStream, oStream, (PhoenixArray)object, noOfElements, baseType, 0);
	}
	
    private byte[] rewriteArrayBytes(byte[] bytes, Integer desiredMaxLength, Integer maxLength, PDataType baseType) {
        if (bytes == null || bytes.length == 0) { return null; }
        ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, bytes.length);
        int initPos = buffer.position();
        buffer.position((buffer.limit() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
        int noOfElemPos = buffer.position();
        int noOfElements = buffer.getInt();
        int temp = noOfElements;
        if (noOfElements < 0) {
            noOfElements = (-noOfElements);
        }
        buffer.position(initPos);
        ByteBuffer newBuffer = ByteBuffer.allocate(Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT
                + (noOfElements * desiredMaxLength));
        for (int i = 0; i < noOfElements; i++) {
            byte[] val =  new byte[(noOfElemPos - initPos)/noOfElements];
            int length = val.length;
            buffer.get(val);
            newBuffer.put(val);
            while(length <  desiredMaxLength) {
                length++;
                newBuffer.put(StringUtil.SPACE_UTF8);
            }
        }
        newBuffer.putInt(temp);
        newBuffer.put(ARRAY_SERIALIZATION_VERSION);
        return newBuffer.array();
    }

    public static int serializeNulls(DataOutputStream oStream, int nulls) throws IOException {
        // We need to handle 3 different cases here
        // 1) Arrays with repeating nulls in the middle which is less than 255
        // 2) Arrays with repeating nulls in the middle which is less than 255 but greater than bytes.MAX_VALUE
        // 3) Arrays with repeating nulls in the middle greaterh than 255
        // Take a case where we have two arrays that has the following elements
        // Array 1 - size : 240, elements = abc, bcd, null, null, bcd,null,null......,null, abc
        // Array 2 - size : 16 : elements = abc, bcd, null, null, bcd, null, null...null, abc
        // In both case the elements and the value array will be the same but the Array 1 is actually smaller because it has more nulls.
        // Now we should have mechanism to show that we treat arrays with more nulls as lesser.  Hence in the above case as 
        // 240 > Bytes.MAX_VALUE, by always inverting the number of nulls we would get a +ve value
        // For Array 2, by inverting we would get a -ve value.  On comparison Array 2 > Array 1.
        // Now for cases where the number of nulls is greater than 255, we would write an those many (byte)1, it is bigger than 255.
        // This would ensure that we don't compare with triple zero which is used as an end  byte
        if (nulls > 0) {
            oStream.write(QueryConstants.SEPARATOR_BYTE);
            int nMultiplesOver255 = nulls / 255;
            while (nMultiplesOver255-- > 0) {
                // Don't write a zero byte, as we need to ensure that the only triple zero
                // byte occurs at the end of the array (i.e. the terminator byte for the
                // element plus the double zero byte at the end of the array).
                oStream.write((byte)1); 
            }
            int nRemainingNulls = nulls % 255; // From 0 to 254
            // Write a byte for the remaining null elements
            if (nRemainingNulls > 0) {
                // Remaining null elements is from 1 to 254.
                // Subtract one and invert so that more remaining nulls becomes smaller than less 
                // remaining nulls and min byte value is always greater than 1, the repeating value  
                // used for arrays with more than 255 repeating null elements.
                // The reason we invert is that  an array with less null elements has a non
                // null element sooner than an array with more null elements. Thus, the more
                // null elements you have, the smaller the array becomes.
                byte nNullByte = SortOrder.invert((byte)(nRemainingNulls-1));
                oStream.write(nNullByte); // Single byte for repeating nulls
            }
        }
        return 0;
    }
 
    public static void writeEndSeperatorForVarLengthArray(DataOutputStream oStream) throws IOException {
        oStream.write(QueryConstants.SEPARATOR_BYTE);
        oStream.write(QueryConstants.SEPARATOR_BYTE);
    }

	public static boolean useShortForOffsetArray(int maxOffset) {
		// If the max offset is less than Short.MAX_VALUE then offset array can use short
		if (maxOffset <= (2 * Short.MAX_VALUE)) {
			return true;
		}
		// else offset array can use Int
		return false;
	}

	public int toBytes(Object object, byte[] bytes, int offset) {
	    PhoenixArray array = (PhoenixArray)object;
        if (array == null || array.baseType == null) {
            return 0;
        }
        return estimateByteSize(object, null, PDataType.fromTypeId((array.baseType.getSqlType() + Types.ARRAY)));
	}

	// Estimates the size of the given array and also calculates the number of nulls and its repetition factor
    public int estimateByteSize(Object o, Pair<Integer, Integer> nullsVsNullRepeationCounter, PDataType baseType) {
        if (baseType.isFixedWidth()) { return baseType.getByteSize(); }
        if (baseType.isArrayType()) {
            PhoenixArray array = (PhoenixArray)o;
            int noOfElements = array.numElements;
            int totalVarSize = 0;
            int nullsRepeationCounter = 0;
            int nulls = 0;
            int totalNulls = 0;
            for (int i = 0; i < noOfElements; i++) {
                totalVarSize += array.estimateByteSize(i);
                if (!PDataType.fromTypeId((baseType.getSqlType() - Types.ARRAY)).isFixedWidth()) {
                    if (array.isNull(i)) {
                        nulls++;
                    } else {
                        if (nulls > 0) {
                            totalNulls += nulls;
                            nulls = 0;
                            nullsRepeationCounter++;
                        }
                    }
                }
            }
            if (nullsVsNullRepeationCounter != null) {
                if (nulls > 0) {
                    totalNulls += nulls;
                    // do not increment nullsRepeationCounter to identify trailing nulls
                }
                nullsVsNullRepeationCounter.setFirst(totalNulls);
                nullsVsNullRepeationCounter.setSecond(nullsRepeationCounter);
            }
            return totalVarSize;
        }
        // Non fixed width types must override this
        throw new UnsupportedOperationException();
    }
    
	public boolean isCoercibleTo(PDataType targetType, Object value) {
	    return targetType.isCoercibleTo(targetType, value);
	}
	
	public boolean isCoercibleTo(PDataType targetType, PDataType expectedTargetType) {
		if(!targetType.isArrayType()) {
			return false;
		} else {
			PDataType targetElementType = PDataType.fromTypeId(targetType.getSqlType()
					- Types.ARRAY);
			PDataType expectedTargetElementType = PDataType.fromTypeId(expectedTargetType
					.getSqlType() - Types.ARRAY);
			return expectedTargetElementType.isCoercibleTo(targetElementType);
		}
    }
	
	public boolean isSizeCompatible(PDataType srcType, Object value,
			byte[] b, Integer maxLength, Integer desiredMaxLength,
			Integer scale, Integer desiredScale) {
		PhoenixArray pArr = (PhoenixArray) value;
		Object[] arr = (Object[]) pArr.array;
		PDataType baseType = PDataType.fromTypeId(srcType.getSqlType()
				- Types.ARRAY);
		for (int i = 0 ; i < arr.length; i++) {
			if (!baseType.isSizeCompatible(baseType, arr[i], b, srcType.getMaxLength(arr[i]),
					desiredMaxLength, scale, desiredScale)) {
				return false;
			}
		}
		return true;
	}
	
	public byte[] coerceBytes(byte[] b, Object value, PDataType actualType, Integer maxLength, Integer scale,
            Integer desiredMaxLength, Integer desiredScale) {
	    PhoenixArray pArr = (PhoenixArray) value;
        Object[] arr = (Object[]) pArr.array;
        PDataType baseType = PDataType.fromTypeId(actualType.getSqlType()
                - Types.ARRAY);
        if (baseType.isFixedWidth()) {
            boolean createNewArray = false;
            for (int i = 0; i < arr.length; i++) {
                if (baseType.getMaxLength(arr[i]) < desiredMaxLength) {
                    createNewArray = true;
                    break;
                }
            }
            if (createNewArray) { return rewriteArrayBytes(b, desiredMaxLength, desiredMaxLength, baseType); }
        }
        return b;
	}


    public Object toObject(String value) {
		// TODO: Do this as done in CSVLoader
		throw new IllegalArgumentException("This operation is not suppported");
	}

	public Object toObject(byte[] bytes, int offset, int length, PDataType baseType, 
			SortOrder sortOrder) {
		return createPhoenixArray(bytes, offset, length, sortOrder,
				baseType);
	}

    public static void positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType baseDataType,
            Integer byteSize) {
        byte[] bytes = ptr.get();
        int initPos = ptr.getOffset();
        int noOfElements = 0;
        noOfElements = Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)),
                Bytes.SIZEOF_INT);
        if (arrayIndex >= noOfElements) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return;
        }
        boolean useShort = true;
        if (noOfElements < 0) {
            noOfElements = -noOfElements;
            useShort = false;
        }

        if (!baseDataType.isFixedWidth()) {
            int indexOffset = Bytes.toInt(bytes,
                    (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT))) + ptr.getOffset();
            if(arrayIndex >= noOfElements) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                // Skip those many offsets as given in the arrayIndex
                // If suppose there are 5 elements in the array and the arrayIndex = 3
                // This means we need to read the 4th element of the array
                // So inorder to know the length of the 4th element we will read the offset of 4th element and the
                // offset of 5th element.
                // Subtracting the offset of 5th element and 4th element will give the length of 4th element
                // So we could just skip reading the other elements.
                int currOffset = getOffset(bytes, arrayIndex, useShort, indexOffset);
                int elementLength = 0;
                if (arrayIndex == (noOfElements - 1)) {
                    elementLength = bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE ? 0 : indexOffset
                            - currOffset - 3;
                } else {
                    elementLength = bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE ? 0 : getOffset(bytes,
                            arrayIndex + 1, useShort, indexOffset) - currOffset - 1;
                }
                ptr.set(bytes, currOffset + initPos, elementLength);
            }
        } else {
            if (byteSize != null) {
                ptr.set(bytes, ptr.getOffset() + (arrayIndex * byteSize), byteSize);
            } else {
                ptr.set(bytes, ptr.getOffset() + (arrayIndex * baseDataType.getByteSize()), baseDataType.getByteSize());
            }
        }
    }

    private static int getOffset(byte[] bytes, int arrayIndex, boolean useShort, int indexOffset) {
        int offset;
        if (useShort) {
            offset = indexOffset + (Bytes.SIZEOF_SHORT * arrayIndex);
            return Bytes.toShort(bytes, offset, Bytes.SIZEOF_SHORT) + Short.MAX_VALUE;
        } else {
            offset = indexOffset + (Bytes.SIZEOF_INT * arrayIndex);
            return Bytes.toInt(bytes, offset, Bytes.SIZEOF_INT);
        }
    }
    
    private static int getOffset(ByteBuffer indexBuffer, int arrayIndex, boolean useShort, int indexOffset ) {
        int offset;
        if(useShort) {
            offset = indexBuffer.getShort() + Short.MAX_VALUE;
        } else {
            offset = indexBuffer.getInt();
        }
        return offset;
    }

	public Object toObject(byte[] bytes, int offset, int length, PDataType baseType) {
		return toObject(bytes, offset, length, baseType, SortOrder.getDefault());
	}
	
	public Object toObject(Object object, PDataType actualType) {
		return object;
	}

	public Object toObject(Object object, PDataType actualType, SortOrder sortOrder) {
		// How to use the sortOrder ? Just reverse the elements
		return toObject(object, actualType);
	}
	
	/**
	 * ser
	 * @param byteStream
	 * @param oStream
	 * @param array
	 * @param noOfElements
	 * @param baseType
	 * @param maxLength 
	 * @param capacity
	 * @return
	 */
    private byte[] createArrayBytes(TrustedByteArrayOutputStream byteStream, DataOutputStream oStream,
            PhoenixArray array, int noOfElements, PDataType baseType, Integer maxLength) {
        try {
            if (!baseType.isFixedWidth()) {
                int[] offsetPos = new int[noOfElements];
                int nulls = 0;
                for (int i = 0; i < noOfElements; i++) {
                    byte[] bytes = array.toBytes(i);
                    if (bytes.length == 0) {
                        offsetPos[i] = byteStream.size();
                        nulls++;
                    } else {
                        nulls = serializeNulls(oStream, nulls);
                        offsetPos[i] = byteStream.size();
                        oStream.write(bytes, 0, bytes.length);
                        oStream.write(QueryConstants.SEPARATOR_BYTE);
                    }
                }
                // Double seperator byte to show end of the non null array
                PArrayDataType.writeEndSeperatorForVarLengthArray(oStream);
                noOfElements = PArrayDataType.serailizeOffsetArrayIntoStream(oStream, byteStream, noOfElements,
                        offsetPos[offsetPos.length - 1], offsetPos);
            } else {
                for (int i = 0; i < noOfElements; i++) {
                    byte[] bytes = array.toBytes(i);
                    int length = bytes.length;
                    if(baseType == PDataType.CHAR) {
                        oStream.write(bytes, 0, length);
                        while(length <=  maxLength) {
                            oStream.writeByte(StringUtil.SPACE_UTF8);
                            length++;
                        }
                    } else {
                        oStream.write(bytes, 0, length);
                    }
                }
            }
            serializeHeaderInfoIntoStream(oStream, noOfElements);
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            ptr.set(byteStream.getBuffer(), 0, byteStream.size());
            return ByteUtil.copyKeyBytesIfNecessary(ptr);
        } catch (IOException e) {
            try {
                byteStream.close();
                oStream.close();
            } catch (IOException ioe) {

            }
        }
        // This should not happen
        return null;
    }

    public static int serailizeOffsetArrayIntoStream(DataOutputStream oStream, TrustedByteArrayOutputStream byteStream,
            int noOfElements, int maxOffset, int[] offsetPos) throws IOException {
        int offsetPosition = (byteStream.size());
        byte[] offsetArr = null;
        boolean useInt = true;
        if (PArrayDataType.useShortForOffsetArray(maxOffset)) {
            offsetArr = new byte[PArrayDataType.initOffsetArray(noOfElements, Bytes.SIZEOF_SHORT)];
            useInt = false;
        } else {
            offsetArr = new byte[PArrayDataType.initOffsetArray(noOfElements, Bytes.SIZEOF_INT)];
            noOfElements = -noOfElements;
        }
        int off = 0;
        if(useInt) {
            for (int pos : offsetPos) {
                Bytes.putInt(offsetArr, off, pos);
                off += Bytes.SIZEOF_INT;
            }
        } else {
            for (int pos : offsetPos) {
                Bytes.putShort(offsetArr, off, (short)(pos - Short.MAX_VALUE));
                off += Bytes.SIZEOF_SHORT;
            }
        }
        oStream.write(offsetArr);
        oStream.writeInt(offsetPosition);
        return noOfElements;
    }

    public static void serializeHeaderInfoIntoBuffer(ByteBuffer buffer, int noOfElements) {
        // No of elements
        buffer.putInt(noOfElements);
        // Version of the array
        buffer.put(ARRAY_SERIALIZATION_VERSION);
    }

    public static void serializeHeaderInfoIntoStream(DataOutputStream oStream, int noOfElements) throws IOException {
        // No of elements
        oStream.writeInt(noOfElements);
        // Version of the array
        oStream.write(ARRAY_SERIALIZATION_VERSION);
    }

	public static int initOffsetArray(int noOfElements, int baseSize) {
		// for now create an offset array equal to the noofelements
		return noOfElements * baseSize;
    }

    // Any variable length array would follow the below order
    // Every element would be seperated by a seperator byte '0'
    // Null elements are counted and once a first non null element appears we
    // write the count of the nulls prefixed with a seperator byte
    // Trailing nulls are not taken into account
    // The last non null element is followed by two seperator bytes
    // For eg
    // a, b, null, null, c, null would be 
    // 65 0 66 0 0 2 67 0 0 0
    // a null null null b c null d would be
    // 65 0 0 3 66 0 67 0 0 1 68 0 0 0
	// Follow the above example to understand how this works
    private Object createPhoenixArray(byte[] bytes, int offset, int length, SortOrder sortOrder, PDataType baseDataType) {
        if (bytes == null || bytes.length == 0) { return null; }
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        int initPos = buffer.position();
        buffer.position((buffer.limit() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
        int noOfElemPos = buffer.position();
        int noOfElements = buffer.getInt();
        boolean useShort = true;
        int baseSize = Bytes.SIZEOF_SHORT;
        if (noOfElements < 0) {
            noOfElements = -noOfElements;
            baseSize = Bytes.SIZEOF_INT;
            useShort = false;
        }
        Object[] elements = (Object[])java.lang.reflect.Array.newInstance(baseDataType.getJavaClass(), noOfElements);
        if (!baseDataType.isFixedWidth()) {
            buffer.position(buffer.limit() - (Bytes.SIZEOF_BYTE + (2 * Bytes.SIZEOF_INT)));
            int indexOffset = buffer.getInt();
            buffer.position(initPos);
            buffer.position(indexOffset + initPos);
            ByteBuffer indexArr = ByteBuffer.allocate(initOffsetArray(noOfElements, baseSize));
            byte[] array = indexArr.array();
            buffer.get(array);
            int countOfElementsRead = 0;
            int i = 0;
            int currOffset = -1;
            int nextOff = -1;
            boolean foundNull = false;
            if (noOfElements != 0) {
                while (countOfElementsRead <= noOfElements) {
                    if (countOfElementsRead == 0) {
                        currOffset = getOffset(indexArr, countOfElementsRead, useShort, indexOffset);
                        countOfElementsRead++;
                    } else {
                        currOffset = nextOff;
                    }
                    if (countOfElementsRead == noOfElements) {
                        nextOff = indexOffset - 2;
                    } else {
                        nextOff = getOffset(indexArr, countOfElementsRead + 1, useShort, indexOffset);
                    }
                    countOfElementsRead++;
                    if ((bytes[currOffset + initPos] != QueryConstants.SEPARATOR_BYTE) && foundNull) {
                        // Found a non null element
                        foundNull = false;
                    }
                    if (bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE) {
                        // Null element
                        foundNull = true;
                        i++;
                        continue;
                    }
                    int elementLength = nextOff - currOffset;
                    buffer.position(currOffset + initPos);
                    // Subtract the seperator from the element length
                    byte[] val = new byte[elementLength - 1];
                    buffer.get(val);
                    elements[i++] = baseDataType.toObject(val, sortOrder);
                }
            }
        } else {
            buffer.position(initPos);
            for (int i = 0; i < noOfElements; i++) {
                byte[] val;
                if (baseDataType == PDataType.CHAR) {
                    int maxLength = (noOfElemPos - initPos)/noOfElements;
                    // Should be char array
                    val = new byte[maxLength];
                } else {
                    val = new byte[baseDataType.getByteSize()];
                }
                buffer.get(val);
                elements[i] = baseDataType.toObject(val, sortOrder);
            }
        }
        return PArrayDataType.instantiatePhoenixArray(baseDataType, elements);
    }
	
    public static PhoenixArray instantiatePhoenixArray(PDataType actualType, Object[] elements) {
        return PDataType.instantiatePhoenixArray(actualType, elements);
    }
	
	public int compareTo(Object lhs, Object rhs) {
		PhoenixArray lhsArr = (PhoenixArray) lhs;
		PhoenixArray rhsArr = (PhoenixArray) rhs;
		if(lhsArr.equals(rhsArr)) {
			return 0;
		}
		return 1;
	}

	public static int getArrayLength(ImmutableBytesWritable ptr,
			PDataType baseType) {
		byte[] bytes = ptr.get();
		if(baseType.isFixedWidth()) {
			return ((ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT))/baseType.getByteSize());
		}
		return Bytes.toInt(bytes, (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)));
	}

    public static int estimateSize(int size, PDataType baseType) {
        if(baseType.isFixedWidth()) {
            return baseType.getByteSize() * size;
        } else {
            return size * ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
        }
        
    }

}