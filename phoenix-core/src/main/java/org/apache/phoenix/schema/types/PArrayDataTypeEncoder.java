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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.ColumnValueEncoder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

public class PArrayDataTypeEncoder implements ColumnValueEncoder {
    static private final int BYTE_ARRAY_DEFAULT_SIZE = 128;

    private PDataType baseType;
    private SortOrder sortOrder;
    private List<Integer> offsetPos;
    private TrustedByteArrayOutputStream byteStream;
    private DataOutputStream oStream;
    private int nulls;
    private byte serializationVersion;
    private boolean rowKeyOrderOptimizable;

    public PArrayDataTypeEncoder(PDataType baseType, SortOrder sortOrder) {
        this(new TrustedByteArrayOutputStream(BYTE_ARRAY_DEFAULT_SIZE), new LinkedList<Integer>(), baseType, sortOrder, true);
    }
    
    public PArrayDataTypeEncoder(TrustedByteArrayOutputStream byteStream, DataOutputStream oStream,
            int numElements, PDataType baseType, SortOrder sortOrder, boolean rowKeyOrderOptimizable, byte serializationVersion) {
        this(byteStream, oStream, new ArrayList<Integer>(numElements), baseType, sortOrder, rowKeyOrderOptimizable, serializationVersion);
    }
    
    public PArrayDataTypeEncoder(TrustedByteArrayOutputStream byteStream, DataOutputStream oStream,
            int numElements, PDataType baseType, SortOrder sortOrder, boolean rowKeyOrderOptimizable) {
        this(byteStream, oStream, new ArrayList<Integer>(numElements), baseType, sortOrder, rowKeyOrderOptimizable, PArrayDataType.SORTABLE_SERIALIZATION_VERSION);
    }
    
    public PArrayDataTypeEncoder(TrustedByteArrayOutputStream byteStream, 
            List<Integer> offsetPos, PDataType baseType, SortOrder sortOrder, boolean rowKeyOrderOptimizable) {
        this(byteStream, new DataOutputStream(byteStream), offsetPos, baseType, sortOrder, rowKeyOrderOptimizable, PArrayDataType.SORTABLE_SERIALIZATION_VERSION);
    }
    
    public PArrayDataTypeEncoder(TrustedByteArrayOutputStream byteStream, DataOutputStream oStream,
            List<Integer> offsetPos, PDataType baseType, SortOrder sortOrder, boolean rowKeyOrderOptimizable, byte serializationVersion) {
        this.baseType = baseType;
        this.sortOrder = sortOrder;
        this.offsetPos = offsetPos;
        this.byteStream = byteStream;
        this.oStream = oStream;
        this.nulls = 0;
        this.serializationVersion = serializationVersion;
        this.rowKeyOrderOptimizable = rowKeyOrderOptimizable;
    }

    private void close() {
        try {
            if (byteStream != null) byteStream.close();
            if (oStream != null) oStream.close();
            byteStream = null;
            oStream = null;
        } catch (IOException ioe) {}
    }
    
    // used to represent the absence of a value 
    @Override
    public void appendAbsentValue() {
        if (serializationVersion == PArrayDataType.IMMUTABLE_SERIALIZATION_VERSION && !baseType.isFixedWidth()) {
            offsetPos.add(-byteStream.size());
            nulls++;
        }
        else {
            throw new UnsupportedOperationException("Cannot represent an absent element");
        }
    }

    public void appendValue(byte[] bytes) {
        appendValue(bytes, 0, bytes.length);
    }

    @Override
    public void appendValue(byte[] bytes, int offset, int len) {
        try {
            // track the offset position here from the size of the byteStream
            if (!baseType.isFixedWidth()) {
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
                if (len == 0) {
                    offsetPos.add(byteStream.size());
                    nulls++;
                } else {
                    nulls = PArrayDataType.serializeNulls(oStream, nulls);
                    offsetPos.add(byteStream.size());
                    if (sortOrder == SortOrder.DESC) {
                        SortOrder.invert(bytes, offset, bytes, offset, len);
                        offset = 0;
                    }
                    oStream.write(bytes, offset, len);
                    if (serializationVersion == PArrayDataType.SORTABLE_SERIALIZATION_VERSION) {
                        oStream.write(PArrayDataType.getSeparatorByte(rowKeyOrderOptimizable, sortOrder));
                    }
                }
            } else {
                // No nulls for fixed length
                if (sortOrder == SortOrder.DESC) {
                    SortOrder.invert(bytes, offset, bytes, offset, len);
                    offset = 0;
                }
                oStream.write(bytes, offset, len);
            }
        } catch (IOException e) {}
    }

    @Override
    public byte[] encode() {
        try {
            if (!baseType.isFixedWidth()) {
                int noOfElements = offsetPos.size();
                int[] offsetPosArray = new int[noOfElements];
                int index = 0;
                for (Integer i : offsetPos) {
                    offsetPosArray[index] = i;
                    ++index;
                }
                if (serializationVersion == PArrayDataType.SORTABLE_SERIALIZATION_VERSION) {
                    // Double seperator byte to show end of the non null array
                    PArrayDataType.writeEndSeperatorForVarLengthArray(oStream, sortOrder, rowKeyOrderOptimizable);
                }
                noOfElements = PArrayDataType.serializeOffsetArrayIntoStream(oStream, byteStream, noOfElements,
                        offsetPosArray[offsetPosArray.length - 1], offsetPosArray, serializationVersion);
                PArrayDataType.serializeHeaderInfoIntoStream(oStream, noOfElements, serializationVersion);
            }
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            ptr.set(byteStream.getBuffer(), 0, byteStream.size());
            return ByteUtil.copyKeyBytesIfNecessary(ptr);
        } catch (IOException e) {} finally {
            close();
        }
        return null;
    }
    
}