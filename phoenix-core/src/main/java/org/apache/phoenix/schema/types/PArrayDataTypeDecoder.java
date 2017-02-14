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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.ColumnValueDecoder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;


public class PArrayDataTypeDecoder implements ColumnValueDecoder {
    
    @Override
    public boolean decode(ImmutableBytesWritable ptr, int index) {
        return PArrayDataTypeDecoder.positionAtArrayElement(ptr, index, PVarbinary.INSTANCE, null);
    }

    public static boolean positionAtArrayElement(Tuple tuple, ImmutableBytesWritable ptr, int index,
            Expression arrayExpr, PDataType pDataType, Integer maxLen) {
        if (!arrayExpr.evaluate(tuple, ptr)) {
            return false;
        } else if (ptr.getLength() == 0) { return true; }
    
        // Given a ptr to the entire array, set ptr to point to a particular element within that array
        // given the type of an array element (see comments in PDataTypeForArray)
        return positionAtArrayElement(ptr, index - 1, pDataType, maxLen);
    }

    public static boolean positionAtArrayElement(ImmutableBytesWritable ptr, int arrayIndex, PDataType baseDataType,
            Integer byteSize) {
        byte[] bytes = ptr.get();
        int initPos = ptr.getOffset();
        if (!baseDataType.isFixedWidth()) {
        	byte serializationVersion = bytes[ptr.getOffset() + ptr.getLength() - Bytes.SIZEOF_BYTE];
            int noOfElements = Bytes.toInt(bytes,
                    (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT)), Bytes.SIZEOF_INT);
            boolean useShort = true;
            if (noOfElements < 0) {
                noOfElements = -noOfElements;
                useShort = false;
            }
            if (arrayIndex >= noOfElements) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return false;
            }
    
            int indexOffset = Bytes.toInt(bytes,
                    (ptr.getOffset() + ptr.getLength() - (Bytes.SIZEOF_BYTE + 2 * Bytes.SIZEOF_INT))) + ptr.getOffset();
            // Skip those many offsets as given in the arrayIndex
            // If suppose there are 5 elements in the array and the arrayIndex = 3
            // This means we need to read the 4th element of the array
            // So inorder to know the length of the 4th element we will read the offset of 4th element and the
            // offset of 5th element.
            // Subtracting the offset of 5th element and 4th element will give the length of 4th element
            // So we could just skip reading the other elements.
            int currOffset = PArrayDataType.getSerializedOffset(bytes, arrayIndex, useShort, indexOffset, serializationVersion);
            if (currOffset<0) {
            	ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return false;
            }
            int elementLength = 0;
            if (arrayIndex == (noOfElements - 1)) {
                int separatorBytes =  serializationVersion == PArrayDataType.SORTABLE_SERIALIZATION_VERSION ? 3 : 0;
                elementLength = (bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE || bytes[currOffset + initPos] == QueryConstants.DESC_SEPARATOR_BYTE) ? 0 : indexOffset
                        - (currOffset + initPos) - separatorBytes;
            } else {
                int separatorByte =  serializationVersion == PArrayDataType.SORTABLE_SERIALIZATION_VERSION ? 1 : 0;
                elementLength = (bytes[currOffset + initPos] == QueryConstants.SEPARATOR_BYTE || bytes[currOffset + initPos] == QueryConstants.DESC_SEPARATOR_BYTE) ? 0 : PArrayDataType.getOffset(bytes,
                        arrayIndex + 1, useShort, indexOffset, serializationVersion) - currOffset - separatorByte;
            }
            ptr.set(bytes, currOffset + initPos, elementLength);
        } else {
            int elemByteSize = (byteSize == null ? baseDataType.getByteSize() : byteSize);
            int offset = arrayIndex * elemByteSize;
            if (offset >= ptr.getLength()) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                ptr.set(bytes, ptr.getOffset() + offset, elemByteSize);
            }
        }
        return true;
    }

}
