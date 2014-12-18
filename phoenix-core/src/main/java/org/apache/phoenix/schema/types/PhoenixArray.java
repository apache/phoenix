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
package org.apache.phoenix.schema.types;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import org.apache.phoenix.util.SQLCloseable;

/**
 * java.sql.Array implementation for Phoenix
 */
public class PhoenixArray implements Array,SQLCloseable {
	PDataType baseType;
	Object array;
	int numElements;
	Integer maxLength;
  protected int hashCode = Integer.MIN_VALUE;
	public PhoenixArray() {
		// empty constructor
	}
	
	public Integer getMaxLength() {
	    return maxLength;
	}

    public boolean isPrimitiveType() {
        return this.baseType.getCodec() != null;
    }

	private static Object[] coerceToNewLength(PDataType baseType, Object[] elements, int maxLength) {
        Object[] resizedElements = new Object[elements.length];
        for (int i = 0; i < elements.length; i++) {
            int length = baseType.getMaxLength(elements[i]);
            if (length == maxLength) {
                resizedElements[i] = elements[i];
            } else {
                resizedElements[i] = baseType.pad(elements[i],maxLength);
            }
        }
        return resizedElements;
	}
	private static Object[] coerceToEqualLength(PDataType baseType, Object[] elements) {
	    if (elements == null || elements.length == 0) {
	        return elements;
	    }
	    Object element = elements[0];
	    int maxLength = baseType.getMaxLength(element);
	    boolean resizeElements = false;
	    for (int i = 1; i < elements.length; i++) {
	        int length = baseType.getMaxLength(elements[i]);
	        if (length > maxLength) {
	            maxLength = length;
	            resizeElements = true;
	        } else if (length < maxLength) {
	            resizeElements = true;
	        }
	    }
	    if (!resizeElements) {
	        return elements;
	    }
	    return coerceToNewLength(baseType, elements, maxLength);
	}
	
	public PhoenixArray(PDataType baseType, Object[] elements) {
		// As we are dealing with primitive types and only the Boxed objects
		this.baseType = baseType;
		if (baseType.isFixedWidth()) {
		    if (baseType.getByteSize() == null) {
    		    elements = coerceToEqualLength(baseType, elements);
    		    if (elements != null && elements.length > 0) {
    		        this.maxLength = baseType.getMaxLength(elements[0]);
    		    }
		    } else {
		        maxLength = baseType.getByteSize();
		    }
		}
    this.array = convertObjectArrayToPrimitiveArray(elements);
		this.numElements = elements.length;
	}
	
	public PhoenixArray(PhoenixArray pArr, Integer desiredMaxLength) {
	    this.baseType = pArr.baseType;
	    Object[] elements = (Object[])pArr.array;
        if (baseType.isFixedWidth()) {
            if (baseType.getByteSize() == null) {
                elements = coerceToNewLength(baseType, (Object[])pArr.array, desiredMaxLength);
                maxLength = desiredMaxLength;
            } else {
                maxLength = baseType.getByteSize();
            }
        }
        this.array = convertObjectArrayToPrimitiveArray(elements);
        this.numElements = elements.length;
    }

  public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
    return elements;
	}
	
	@Override
	public void free() throws SQLException {
	}

	@Override
	public Object getArray() throws SQLException {
		return array;
	}
	
	@Override
	public void close() throws SQLException {
		this.array = null;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException("Currently not supported");
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		if(index < 1) {
			throw new IllegalArgumentException("Index cannot be less than 1");
		}
		// Get the set of elements from the given index to the specified count
		Object[] intArr = (Object[]) array;
		boundaryCheck(index, count, intArr);
		Object[] newArr = new Object[count];
		// Add checks() here.
		int i = 0;
		for (int j = (int) index; j < count; j++) {
			newArr[i] = intArr[j];
			i++;
		}
		return newArr;
	}

	private void boundaryCheck(long index, int count, Object[] arr) {
		if ((--index) + count > arr.length) {
			throw new IllegalArgumentException("The array index is out of range of the total number of elements in the array " + arr.length);
		}
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map)
			throws SQLException {
		if(map != null && !map.isEmpty()) {
			throw new UnsupportedOperationException("Currently not supported");
		}
		return null;
	}

	@Override
	public int getBaseType() throws SQLException {
		return baseType.getSqlType();
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return baseType.getSqlTypeName();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		throw new UnsupportedOperationException("Currently not supported");
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> arg0)
			throws SQLException {
		throw new UnsupportedOperationException("Currently not supported");
	}

	@Override
	public ResultSet getResultSet(long arg0, int arg1) throws SQLException {
		throw new UnsupportedOperationException("Currently not supported");
	}

	@Override
	public ResultSet getResultSet(long arg0, int arg1,
			Map<String, Class<?>> arg2) throws SQLException {
		throw new UnsupportedOperationException("Currently not supported");
	}

  /**
   * Return the value in position {@code index} from the underlying array. Used to work around
   * casting and reflection while enabling primitive arrays.
   */
  public Object getElement(int index) {
    return ((Object[]) array)[index];
  }

	public int getDimensions() {
		return this.numElements;
	}
	
	public int estimateByteSize(int pos) {
	    if(((Object[])array)[pos] == null) {
	        return 0;
	    }
		return this.baseType.estimateByteSize(((Object[])array)[pos]);
	}
	
	public Integer getMaxLength(int pos) {
	    return this.baseType.getMaxLength(((Object[])array)[pos]);
	}
	
	public byte[] toBytes(int pos) {
		return this.baseType.toBytes(((Object[])array)[pos]);
	}
	
	public boolean isNull(int pos) {
	    if(this.baseType.toBytes(((Object[])array)[pos]).length == 0) {
	        return true;
	    } else {
	        return false;
	    }
	}
	
	@Override
	public boolean equals(Object obj) {
    if (obj == null) return false;
    if (this == obj) return true;
    if (!(obj instanceof PhoenixArray)) return false;
    PhoenixArray oArray = (PhoenixArray) obj;
    if (numElements != oArray.numElements) return false;
    if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
    return Arrays.deepEquals((Object[]) array, (Object[]) oArray.array);
	}

	@Override
	public int hashCode() {
    // implementation based on commons.lang.HashCodeBuilder, except the hashcode is cached and
    // reused for a given instance.
    if (hashCode != Integer.MIN_VALUE) return hashCode;
    hashCode = 17;
    hashCode = hashCode * 37 + numElements;
    hashCode = hashCode * 37 + baseType.getSqlType();
    hashCode = hashCode * 37 + Arrays.deepHashCode((Object[]) array);
    return hashCode;
	}

	public static class PrimitiveIntPhoenixArray extends PhoenixArray {
		private int[] intArr;
		public PrimitiveIntPhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			intArr = new int[elements.length];
			int i = 0;
			for(Object o : elements) {
			    if (o != null) {
			        intArr[i] = (Integer)o;
			    }
			    i++;
			}
      return intArr;
		}
		
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(intArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(intArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveIntPhoenixArray oArray = (PrimitiveIntPhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((int[]) array, (int[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((int[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((int[]) array)[index];
    }
	}
	
	public static class PrimitiveShortPhoenixArray extends PhoenixArray {
		private short[] shortArr;
		public PrimitiveShortPhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			shortArr = new short[elements.length];
			int i = 0;
            for(Object o : elements) {
                if (o != null) {
                    shortArr[i] = (Short)o;
                }
                i++;
            }
      return shortArr;
		}
		
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(shortArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(shortArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveShortPhoenixArray oArray = (PrimitiveShortPhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((short[]) array, (short[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((short[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((short[]) array)[index];
    }
  }
	
	public static class PrimitiveLongPhoenixArray extends PhoenixArray {
		private long[] longArr;
		public PrimitiveLongPhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			longArr = new long[elements.length];
			int i = 0;
            for(Object o : elements) {
                if (o != null) {
                    longArr[i] = (Long)o;
                }
                i++;
            }
      return longArr;
		}
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(longArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(longArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveLongPhoenixArray oArray = (PrimitiveLongPhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((long[]) array, (long[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((long[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((long[]) array)[index];
    }
	}
	
	public static class PrimitiveDoublePhoenixArray extends PhoenixArray {
		private double[] doubleArr;
		public PrimitiveDoublePhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			doubleArr = new double[elements.length];
			int i = 0;
			for (Object o : elements) {
			    if (o != null) {
			        doubleArr[i] = (Double) o;
			    }
			    i++;
			}
      return doubleArr;
		}
		
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(doubleArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(doubleArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveDoublePhoenixArray oArray = (PrimitiveDoublePhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((double[]) array, (double[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((double[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((double[]) array)[index];
    }
	}
	
	public static class PrimitiveFloatPhoenixArray extends PhoenixArray {
		private float[] floatArr;
		public PrimitiveFloatPhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			floatArr = new float[elements.length];
			int i = 0;
            for(Object o : elements) {
                if (o != null) {
                    floatArr[i] = (Float)o;
                }
                i++;
            }
      return floatArr;
		}
		
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(floatArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(floatArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveFloatPhoenixArray oArray = (PrimitiveFloatPhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((float[]) array, (float[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((float[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((float[]) array)[index];
    }
	}
	
	public static class PrimitiveBytePhoenixArray extends PhoenixArray {
		private byte[] byteArr;
		public PrimitiveBytePhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			byteArr = new byte[elements.length];
			int i = 0;
            for(Object o : elements) {
                if (o != null) {
                    byteArr[i] = (Byte)o;
                }
                i++;
            }
      return byteArr;
		}
		
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(byteArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(byteArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveBytePhoenixArray oArray = (PrimitiveBytePhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((byte[]) array, (byte[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((byte[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((byte[]) array)[index];
    }
	}
	
	public static class PrimitiveBooleanPhoenixArray extends PhoenixArray {
		private boolean[] booleanArr;
		public PrimitiveBooleanPhoenixArray(PDataType dataType, Object[] elements) {
			super(dataType, elements);
		}
		@Override
		public Object convertObjectArrayToPrimitiveArray(Object[] elements) {
			booleanArr = new boolean[elements.length];
			int i = 0;
            for(Object o : elements) {
                if (o != null) {
                    booleanArr[i] = (Boolean)o;
                }
                i++;
            }
      return booleanArr;
		}
		
		@Override
        public int estimateByteSize(int pos) {
			return this.baseType.estimateByteSize(booleanArr[pos]);
		}
		
		@Override
        public byte[] toBytes(int pos) {
			return this.baseType.toBytes(booleanArr[pos]);
		}
		
		@Override
		public boolean equals(Object obj) {
      if (obj == null) return false;
      if (this == obj) return true;
      if (this.getClass() != obj.getClass()) return false;
      PrimitiveBooleanPhoenixArray oArray = (PrimitiveBooleanPhoenixArray) obj;
      if (numElements != oArray.numElements) return false;
      if (baseType.getSqlType() != oArray.baseType.getSqlType()) return false;
      return Arrays.equals((boolean[]) array, (boolean[]) oArray.array);
		}

    @Override
    public int hashCode() {
      if (hashCode != Integer.MIN_VALUE) return hashCode;
      hashCode = 17;
      hashCode = hashCode * 37 + numElements;
      hashCode = hashCode * 37 + baseType.getSqlType();
      hashCode = hashCode * 37 + Arrays.hashCode((boolean[]) array);
      return hashCode;
    }

    @Override
    public Object getElement(int index) {
      return ((boolean[]) array)[index];
    }
	}
}
