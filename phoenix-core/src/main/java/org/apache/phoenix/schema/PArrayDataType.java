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

import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.util.ByteUtil;

/**
 * The datatype for PColummns that are Arrays
 */
public class PArrayDataType {

    private static final int MAX_POSSIBLE_VINT_LENGTH = 2;
    private static final byte ARRAY_SERIALIZATION_VERSION = 1;
	public PArrayDataType() {
	}

	public byte[] toBytes(Object object, PDataType baseType) {
		if(object == null) {
			throw new ConstraintViolationException(this + " may not be null");
		}
	    int size = PDataType.fromTypeId((baseType.getSqlType() + Types.ARRAY)).estimateByteSize(object);
	    
        int noOfElements = ((PhoenixArray)object).numElements;
        if(noOfElements == 0) {
        	return ByteUtil.EMPTY_BYTE_ARRAY;
        }
        ByteBuffer buffer;
        int capacity = 0;
		if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
			// variable
			if (calculateMaxOffset(size)) {
				// Use Short to represent the offset
				capacity = initOffsetArray(noOfElements, Bytes.SIZEOF_SHORT);
			} else {
				capacity = initOffsetArray(noOfElements, Bytes.SIZEOF_INT);
				// Negate the number of elements
				noOfElements = -noOfElements;
			}
			buffer = ByteBuffer.allocate(size + capacity + Bytes.SIZEOF_INT+ Bytes.SIZEOF_BYTE);
		} else {
			buffer = ByteBuffer.allocate(size);
		}
		return bytesFromByteBuffer((PhoenixArray)object, buffer, noOfElements, baseType, capacity);
	}

	private boolean calculateMaxOffset(int size) {
		// If the total size + Offset postion ptr + Numelements in Vint is less than Short
		if ((size + Bytes.SIZEOF_INT + MAX_POSSIBLE_VINT_LENGTH) <= (2 * Short.MAX_VALUE)) {
			return true;
		}
		return false;
	}

	public int toBytes(Object object, byte[] bytes, int offset) {
	    PhoenixArray array = (PhoenixArray)object;
        if (array == null || array.baseType == null) {
            return 0;
        }
	    return PDataType.fromTypeId((array.baseType.getSqlType() + Types.ARRAY)).estimateByteSize(object);
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
		Object[] charArr = (Object[]) pArr.array;
		PDataType baseType = PDataType.fromTypeId(srcType.getSqlType()
				- Types.ARRAY);
		for (int i = 0 ; i < charArr.length; i++) {
			if (!baseType.isSizeCompatible(baseType, value, b, maxLength,
					desiredMaxLength, scale, desiredScale)) {
				return false;
			}
		}
		return true;
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
	
	public static void positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType baseDataType) {
		byte[] bytes = ptr.get();
		int initPos = ptr.getOffset();
		int noOfElements = 0;
		noOfElements = Bytes.toInt(bytes, ptr.getOffset() + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_INT);
		int noOFElementsSize = Bytes.SIZEOF_INT;
		if(arrayIndex >= noOfElements) {
			throw new IndexOutOfBoundsException(
					"Invalid index "
							+ arrayIndex
							+ " specified, greater than the no of elements in the array: "
							+ noOfElements);
		}
		boolean useShort = true;
		int baseSize = Bytes.SIZEOF_SHORT;
		if (noOfElements < 0) {
			noOfElements = -noOfElements;
			baseSize = Bytes.SIZEOF_INT;
			useShort = false;
		}

		if (baseDataType.getByteSize() == null) {
			int offset = ptr.getOffset() + noOFElementsSize + Bytes.SIZEOF_BYTE;
			int indexOffset = Bytes.toInt(bytes, offset) + ptr.getOffset();
			int valArrayPostion = offset + Bytes.SIZEOF_INT;
			offset += Bytes.SIZEOF_INT;
			int currOff = 0;
			if (noOfElements > 1) {
				while (offset <= (initPos+ptr.getLength())) {
					int nextOff = 0;
					// Skip those many offsets as given in the arrayIndex
					// If suppose there are 5 elements in the array and the arrayIndex = 3
					// This means we need to read the 4th element of the array
					// So inorder to know the length of the 4th element we will read the offset of 4th element and the offset of 5th element.
					// Subtracting the offset of 5th element and 4th element will give the length of 4th element
					// So we could just skip reading the other elements.
					if(useShort) {
						// If the arrayIndex is already the last element then read the last before one element and the last element
						offset = indexOffset + (Bytes.SIZEOF_SHORT * arrayIndex);
						if (arrayIndex == (noOfElements - 1)) {
							currOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							nextOff = indexOffset;
							offset += baseSize;
						} else {
							currOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							offset += baseSize;
							nextOff = Bytes.toShort(bytes, offset, baseSize) + Short.MAX_VALUE;
							offset += baseSize;
						}
					} else {
						// If the arrayIndex is already the last element then read the last before one element and the last element
						offset = indexOffset + (Bytes.SIZEOF_INT * arrayIndex);
						if (arrayIndex == (noOfElements - 1)) {
							currOff = Bytes.toInt(bytes, offset, baseSize);
							nextOff = indexOffset;
							offset += baseSize;
						} else {
							currOff = Bytes.toInt(bytes, offset, baseSize);
							offset += baseSize;
							nextOff = Bytes.toInt(bytes, offset, baseSize);
							offset += baseSize;
						}
					}
					int elementLength = nextOff - currOff;
					ptr.set(bytes, currOff + initPos, elementLength);
					break;
				}
			} else {
				ptr.set(bytes, valArrayPostion + initPos, indexOffset - valArrayPostion);
			}
		} else {
			ptr.set(bytes,
					ptr.getOffset() + arrayIndex * baseDataType.getByteSize()
							+ noOFElementsSize + Bytes.SIZEOF_BYTE, baseDataType.getByteSize());
		}
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
	
	// Making this private
	/**
	 * The format of the byte buffer looks like this for variable length array elements
	 * <noofelements><Offset of the index array><elements><offset array>
	 * where <noOfelements> - vint
	 * <offset of the index array> - int
	 * <elements>  - these are the values
	 * <offset array> - offset of every element written as INT/SHORT
	 * 
	 * @param array
	 * @param buffer
	 * @param noOfElements
	 * @param byteSize
	 * @param capacity 
	 * @return
	 */
	private byte[] bytesFromByteBuffer(PhoenixArray array, ByteBuffer buffer,
			int noOfElements, PDataType baseType, int capacity) {
		int temp = noOfElements;
        if (buffer == null) return null;
        buffer.put(ARRAY_SERIALIZATION_VERSION);
        buffer.putInt(noOfElements);
        if (!baseType.isFixedWidth() || baseType.isCoercibleTo(PDataType.VARCHAR)) {
            int fillerForOffsetByteArray = buffer.position();
            buffer.position(fillerForOffsetByteArray + Bytes.SIZEOF_INT);
            ByteBuffer offsetArray = ByteBuffer.allocate(capacity);
            if(noOfElements < 0){
            	noOfElements = -noOfElements;
            }
            for (int i = 0; i < noOfElements; i++) {
                // Not fixed width
				if (temp < 0) {
					offsetArray.putInt(buffer.position());
				} else {
					offsetArray.putShort((short)(buffer.position() - Short.MAX_VALUE));
				}
                byte[] bytes = array.toBytes(i);
                buffer.put(bytes);
            }
            int offsetArrayPosition = buffer.position();
            buffer.put(offsetArray.array());
            buffer.position(fillerForOffsetByteArray);
            buffer.putInt(offsetArrayPosition);
        } else {
            for (int i = 0; i < noOfElements; i++) {
                byte[] bytes = array.toBytes(i);
                buffer.put(bytes);
            }
        }
        return buffer.array();
	}

	private static int initOffsetArray(int noOfElements, int baseSize) {
		// for now create an offset array equal to the noofelements
		return noOfElements * baseSize;
    }

	private Object createPhoenixArray(byte[] bytes, int offset, int length,
			SortOrder sortOrder, PDataType baseDataType) {
		if(bytes == null || bytes.length == 0) {
			return null;
		}
		ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
		int initPos = buffer.position();
		buffer.get();
		int noOfElements = buffer.getInt();
		boolean useShort = true;
		int baseSize = Bytes.SIZEOF_SHORT;
		if(noOfElements < 0) {
			noOfElements = -noOfElements;
			baseSize = Bytes.SIZEOF_INT;
			useShort = false;
		}
		Object[] elements = (Object[]) java.lang.reflect.Array.newInstance(
				baseDataType.getJavaClass(), noOfElements);
		if (!baseDataType.isFixedWidth() || baseDataType.isCoercibleTo(PDataType.VARCHAR)) {
			int indexOffset = buffer.getInt();
			int valArrayPostion = buffer.position();
			buffer.position(indexOffset + initPos);
			ByteBuffer indexArr = ByteBuffer
					.allocate(initOffsetArray(noOfElements, baseSize));
			byte[] array = indexArr.array();
			buffer.get(array);
			int countOfElementsRead = 0;
			int i = 0;
			int currOff = -1;
			int nextOff = -1;
			if (noOfElements > 1) {
				while (indexArr.hasRemaining()) {
					if (countOfElementsRead < noOfElements) {
						if (currOff == -1) {
							if ((indexArr.position() + 2 * baseSize) <= indexArr
									.capacity()) {
								if (useShort) {
									currOff = indexArr.getShort() + Short.MAX_VALUE;
									nextOff = indexArr.getShort() + Short.MAX_VALUE;
								} else {
									currOff = indexArr.getInt();
									nextOff = indexArr.getInt();
								}
								countOfElementsRead += 2;
							}
						} else {
							currOff = nextOff;
							if(useShort) {
								nextOff = indexArr.getShort() + Short.MAX_VALUE;
							} else {
								nextOff = indexArr.getInt();
							}
							countOfElementsRead += 1;
						}
						int elementLength = nextOff - currOff;
						buffer.position(currOff + initPos);
						byte[] val = new byte[elementLength];
						buffer.get(val);
						elements[i++] = baseDataType.toObject(val,
								sortOrder);
					}
				}
				buffer.position(nextOff + initPos);
				byte[] val = new byte[indexOffset - nextOff];
				buffer.get(val);
				elements[i++] = baseDataType.toObject(val, sortOrder);
			} else {
				byte[] val = new byte[indexOffset - valArrayPostion];
				buffer.position(valArrayPostion + initPos);
				buffer.get(val);
				elements[i++] = baseDataType.toObject(val, sortOrder);
			}
		} else {
			for (int i = 0; i < noOfElements; i++) {
				byte[] val;
				if (baseDataType.getByteSize() == null) {
					val = new byte[length];
				} else {
					val = new byte[baseDataType.getByteSize()];
				}
				buffer.get(val);
				elements[i] = baseDataType.toObject(val, sortOrder);
			}
		}
		return PArrayDataType
				.instantiatePhoenixArray(baseDataType, elements);
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
		return Bytes.toInt(bytes, ptr.getOffset() + Bytes.SIZEOF_BYTE);
	}

}