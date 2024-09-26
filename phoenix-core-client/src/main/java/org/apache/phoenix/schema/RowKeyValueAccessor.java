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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.SchemaUtil;

/**
 * 
 * Class that encapsulates accessing a value stored in the row key.
 *
 * 
 * @since 0.1
 */
public class RowKeyValueAccessor implements Writable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowKeyValueAccessor.class);

    /**
     * Constructor solely for use during deserialization. Should not
     * otherwise be used.
     */
    public RowKeyValueAccessor() {
    }

    /**
     * The class used to keep list of booleans representing whether the data type of the given
     * field is of type VARBINARY_ENCODED. For example, let's say we have variable length
     * data types in the order (VARCHAR, VARBINARY_ENCODED, VARCHAR). The corresponding object
     * of this class will have binaryEncodedDataTypes list with values (false, true, false).
     * The array of this class is serialized and deserialized together with other fields of
     * RowKeyValueAccessor. The value of the list determines which separator bytes need to be
     * used while iterating through the rowkey bytes.
     */
    static class ListOfEncodedTypeFlags {
        private final List<Boolean> binaryEncodedDataTypes = new ArrayList<>();

        public void addBinaryEncodedDataTypes(boolean val) {
            this.binaryEncodedDataTypes.add(val);
        }

        public List<Boolean> getBinaryEncodedDataTypes() {
            return binaryEncodedDataTypes;
        }
    }

    /**
     * The class used to keep list of booleans representing whether the order of the given
     * field is Ascending. For example, let's say we have rowkey with pk columns
     * (A ASC, B DESC, C ASC). The corresponding object of this class will have sortOrderAsc list
     * with values (true, false, true). The array of this class is serialized and deserialized
     * together with other fields of RowKeyValueAccessor. The value of the list determines which
     * separator bytes need to be used while iterating through the rowkey bytes.
     */
    static class SortOrderList {
        private final List<Boolean> sortOrderAsc = new ArrayList<>();

        public void addSortOrderAsc(boolean val) {
            this.sortOrderAsc.add(val);
        }

        public List<Boolean> getSortOrderAsc() {
            return sortOrderAsc;
        }
    }

    /**
     * Constructor to compile access to the value in the row key formed from
     * a list of PData.
     * 
     * @param data the list of data that make up the key
     * @param index the zero-based index of the data item to access.
     */
    public RowKeyValueAccessor(List<? extends PDatum> data, int index) {
        this.index = index;
        int[] offsets = new int[data.size()];
        ListOfEncodedTypeFlags[] listOfEncodedTypesLists =
            new ListOfEncodedTypeFlags[data.size()];
        SortOrderList[] sortOrderLists = new SortOrderList[data.size()];
        int nOffsets = 0;
        Iterator<? extends PDatum> iterator = data.iterator();
        PDatum datum = iterator.next();
        int pos = 0;
        while (pos < index) {
            int offset = 0;
            if (datum.getDataType().isFixedWidth()) {
                // For continuous fixed width data type columns, accumulate how many
                // of them contains ASC and DESC order types.
                ListOfEncodedTypeFlags encodedTypesLists = new ListOfEncodedTypeFlags();
                SortOrderList sortOrders = new SortOrderList();
                do {
                    encodedTypesLists.addBinaryEncodedDataTypes(false);
                    sortOrders.addSortOrderAsc(datum.getSortOrder() == SortOrder.ASC);
                    // For non parameterized types such as BIGINT, the type will return its max length.
                    // For parameterized types, for example CHAR(10) the type cannot know the max length,
                    // so in this case, the max length is retrieved from the datum.
                    Integer maxLength = datum.getDataType().getByteSize(); 
                    offset += maxLength == null ? datum.getMaxLength() : maxLength;
                    datum = iterator.next();
                    pos++;
                } while (pos < index && datum.getDataType().isFixedWidth());
                offsets[nOffsets] = offset; // Encode fixed byte offset as positive
                listOfEncodedTypesLists[nOffsets] = encodedTypesLists;
                sortOrderLists[nOffsets++] = sortOrders;
            } else {
                // For continuous variable length data type columns, accumulate how many
                // of them contains ASC and DESC order types. And how many of them contains
                // VARBINARY_ENCODED and other variable length types. This information is
                // crucial to figure out which separator bytes to use while going through each
                // column value.
                ListOfEncodedTypeFlags encodedTypesLists = new ListOfEncodedTypeFlags();
                SortOrderList sortOrders = new SortOrderList();
                do {
                    encodedTypesLists.addBinaryEncodedDataTypes(
                        datum.getDataType() == PVarbinaryEncoded.INSTANCE);
                    sortOrders.addSortOrderAsc(datum.getSortOrder() == SortOrder.ASC);
                    offset++; // Count the number of variable length columns
                    datum = iterator.next();
                    pos++;
                } while (pos < index && !datum.getDataType().isFixedWidth());
                offsets[nOffsets] = -offset; // Encode number of variable length columns as negative
                listOfEncodedTypesLists[nOffsets] = encodedTypesLists;
                sortOrderLists[nOffsets++] = sortOrders;
            }
        }
        if (nOffsets < offsets.length) {
            this.offsets = Arrays.copyOf(offsets, nOffsets);
            this.listOfEncodedTypesLists = Arrays.copyOf(listOfEncodedTypesLists, nOffsets);
            this.sortOrderLists = Arrays.copyOf(sortOrderLists, nOffsets);
        } else {
            this.offsets = offsets;
            this.listOfEncodedTypesLists = listOfEncodedTypesLists;
            this.sortOrderLists = sortOrderLists;
        }
        // Remember this so that we don't bother looking for the null separator byte in this case
        this.isFixedLength = datum.getDataType().isFixedWidth();
        this.hasSeparator = !isFixedLength && iterator.hasNext();
    }
    
    RowKeyValueAccessor(int[] offsets, boolean isFixedLength, boolean hasSeparator) {
        this.offsets = offsets;
        this.isFixedLength = isFixedLength;
        this.hasSeparator = hasSeparator;
    }

    private int index = -1; // Only available on client side
    private int[] offsets;
    /**
     * An array of BinaryEncodedTypesLists. Each element of BinaryEncodedTypesLists consists of
     * list of booleans representing whether the given column is of type VARBINARY_ENCODED or other
     * variable length data type. All the continuous fixed length as well as variable length
     * data type columns are clubbed together. For example, let's say we have columns
     * (A INTEGER, B FLOAT, C DOUBLE, D VARCHAR, E VARBINARY_ENCODED, F VARCHAR). Here, we have
     * three continuous fixed-length columns and three continuous variable length columns. Hence,
     * binaryEncodedTypesLists array consists of only two elements. binaryEncodedTypesLists[0]
     * consists of List<Boolean> as [false, false, false] because INTEGER, FLOAT and DOUBLE are of
     * fixed length data type. Whereas binaryEncodedTypesLists[1] consists of List<Boolean> as
     * [false, true, false] because D and F are not VARBINARY_ENCODED whereas E is
     * VARBINARY_ENCODED.
     */
    private ListOfEncodedTypeFlags[] listOfEncodedTypesLists;
    /**
     * An array of SortOrderLists. Each element of SortOrderLists consists of list of booleans
     * representing whether the given column is ASC of DESC order. All the continuous fixed length
     * as well as variable length data type columns are clubbed together. For example, let's say
     * we have columns
     * (A INTEGER, B FLOAT DESC, C DOUBLE, D VARCHAR DESC, E VARBINARY_ENCODED, F VARCHAR).
     * Here, we have three continuous fixed-length columns and three continuous variable length
     * columns. Hence, sortOrderLists array consists of only two elements. sortOrderLists[0]
     * consists of List<Boolean> as [true, false, true] because A and C are ASC whereas B is DESC
     * ordered column. sortOrderLists[1] consists of List<Boolean> as [false, true, true] because
     * D is DESC whereas E and F are ASC ordered columns.
     */
    private SortOrderList[] sortOrderLists;
    private boolean isFixedLength;
    private boolean hasSeparator;

    public int getIndex() {
        return index;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (hasSeparator ? 1231 : 1237);
        result = prime * result + (isFixedLength ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(offsets);
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        RowKeyValueAccessor other = (RowKeyValueAccessor)obj;
        if (hasSeparator != other.hasSeparator) return false;
        if (isFixedLength != other.isFixedLength) return false;
        if (!Arrays.equals(offsets, other.offsets)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowKeyValueAccessor [offsets=" + Arrays.toString(offsets) + ", isFixedLength=" + isFixedLength
                + ", hasSeparator=" + hasSeparator + "]";
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // Decode hasSeparator and isFixedLength from vint storing offset array length
        int length = WritableUtils.readVInt(input);
        hasSeparator = (length & 0x02) != 0;
        isFixedLength = (length & 0x01) != 0;
        length >>= 2;
        offsets = ByteUtil.deserializeVIntArray(input, length);

        this.listOfEncodedTypesLists = null;
        this.sortOrderLists = null;

        // New client that supports new structure of RowKeyValueAccessor with additional fields
        // need to differentiate serialization from old client that does not support additional
        // fields of RowKeyValueAccessor. This is specifically required because multiple
        // Expressions are serialized and deserialized together.
        // We expect to use DataInputBuffer or DataInputStream only. Both of them support
        // reading bytes from the byte stream without moving the cursor. While DataInputBuffer
        // provides access to the underlying byte buffer, DataInputStream supports mark and reset
        // functions to read the bytes and re-adjust the cursor back to original position.
        // If mark is not supported, we should not risk reading additional bytes as it can
        // move the cursor forward and the deserialization of other Expressions can fail.
        // It is very important to read the separator bytes
        // ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR without moving the pointer forward. If the
        // pointer is moved forward, old client that does not support new fields of
        // RowKeyValueAccessor would likely fail while running against new server that supports
        // new fields of RowKeyValueAccessor.
        if (input instanceof DataInputBuffer) {
            DataInputBuffer dataInputBuffer = (DataInputBuffer) input;
            int offset = dataInputBuffer.getPosition();
            int len = dataInputBuffer.getLength();
            if ((offset + QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length) > len) {
                return;
            }
            byte[] data = dataInputBuffer.getData();
            if (!Bytes.equals(data, offset,
                QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length,
                QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR, 0,
                QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length)) {
                return;
            }
        } else if (input instanceof DataInputStream) {
            DataInputStream dataInputStream = (DataInputStream) input;
            if (dataInputStream.markSupported()) {
                dataInputStream.mark(
                    QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length * 1000);
                byte[] data =
                    new byte[QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length];
                try {
                    int bytesRead = dataInputStream.read(data);
                    if (bytesRead
                        != QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length) {
                        dataInputStream.reset();
                        return;
                    }
                } catch (IOException e) {
                    // This can happen if EOF is reached while reading the data i.e. the stream
                    // does not have ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length num of bytes.
                    dataInputStream.reset();
                    return;
                }
                if (!Bytes.equals(data, 0,
                    QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length,
                    QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR, 0,
                    QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length)) {
                    dataInputStream.reset();
                    return;
                }
                dataInputStream.reset();
            } else {
                LOGGER.warn("DataInputStream {} does not support mark.", dataInputStream);
                return;
            }
        } else {
            LOGGER.error("Type of DataInput is neither DataInputBuffer nor DataInputStream. "
                + "This is not expected. Do not attempt deserialization of "
                + "binaryEncodedTypesLists and sortOrderLists for compatibility purpose."
                + " input: {}", input);
            return;
        }

        byte[] bytes = new byte[QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR.length];
        input.readFully(bytes, 0, bytes.length);

        int binaryEncodedTypesListsLen = WritableUtils.readVInt(input);
        if (binaryEncodedTypesListsLen == 0) {
            this.listOfEncodedTypesLists = new ListOfEncodedTypeFlags[0];
        } else {
            byte[] binaryEncodedTypesListsBytes = new byte[binaryEncodedTypesListsLen];
            input.readFully(binaryEncodedTypesListsBytes, 0, binaryEncodedTypesListsLen);
            this.listOfEncodedTypesLists = JacksonUtil.getObjectReader()
                .readValue(binaryEncodedTypesListsBytes, ListOfEncodedTypeFlags[].class);
        }

        int sortOrderListsLen = WritableUtils.readVInt(input);
        if (sortOrderListsLen == 0) {
            this.sortOrderLists = new SortOrderList[0];
        } else {
            byte[] sortOrdersListsBytes = new byte[sortOrderListsLen];
            input.readFully(sortOrdersListsBytes, 0, sortOrderListsLen);
            this.sortOrderLists = JacksonUtil.getObjectReader()
                .readValue(sortOrdersListsBytes, SortOrderList[].class);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // Encode hasSeparator and isFixedLength into vint storing offset array length
        // (since there's plenty of room)
        int length = offsets.length << 2;
        length |= (hasSeparator ? 1 << 1 : 0) | (isFixedLength ? 1 : 0);
        ByteUtil.serializeVIntArray(output, offsets, length);

        // New client that supports new structure of RowKeyValueAccessor with additional fields
        // need to differentiate serialization from old client that does not support additional
        // fields of RowKeyValueAccessor. This is specifically required because multiple
        // Expressions are serialized and deserialized together.
        // So, let's write separator bytes as ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR, followed
        // by serialization of new fields (binaryEncodedTypesLists and sortOrderLists).
        output.write(QueryConstants.ROW_KEY_VAL_ACCESSOR_NEW_FIELDS_SEPARATOR);
        if (this.listOfEncodedTypesLists.length == 0) {
            WritableUtils.writeVInt(output, 0);
        } else {
            byte[] binaryEncodedTypesListsBytes =
                JacksonUtil.getObjectWriter().writeValueAsBytes(this.listOfEncodedTypesLists);
            WritableUtils.writeVInt(output, binaryEncodedTypesListsBytes.length);
            if (binaryEncodedTypesListsBytes.length > 0) {
                output.write(binaryEncodedTypesListsBytes);
            }
        }

        if (this.sortOrderLists.length == 0) {
            WritableUtils.writeVInt(output, 0);
        } else {
            byte[] sortOrdersListsBytes =
                JacksonUtil.getObjectWriter().writeValueAsBytes(this.sortOrderLists);
            WritableUtils.writeVInt(output, sortOrdersListsBytes.length);
            if (sortOrdersListsBytes.length > 0) {
                output.write(sortOrdersListsBytes);
            }
        }
    }
    
    private static boolean isSeparatorByte(byte b) {
        return b == QueryConstants.SEPARATOR_BYTE || b == QueryConstants.DESC_SEPARATOR_BYTE;
    }

    /**
     * Calculate the byte offset in the row key to the start of the PK column value
     * @param keyBuffer the byte array of the row key
     * @param keyOffset the offset in the byte array of where the key begins
     * @return byte offset to the start of the PK column value
     */
    public int getOffset(byte[] keyBuffer, int keyOffset) {
        if (this.listOfEncodedTypesLists == null && this.sortOrderLists == null) {
            return getOffsetWithNullTypeAndOrderInfo(keyBuffer, keyOffset);
        }
        // Use encoded offsets to navigate through row key buffer
        for (int i = 0; i < offsets.length; i++) {
            int offset = offsets[i];
            ListOfEncodedTypeFlags binaryEncodedTypesList = this.listOfEncodedTypesLists[i];
            SortOrderList sortOrderList = this.sortOrderLists[i];
            if (offset >= 0) { // If offset is non negative, it's a byte offset
                keyOffset += offset;
            } else { // Else, a negative offset is the number of variable length values to skip
                int pos = 0;
                while (offset++ < 0) {
                    boolean isVarBinaryEncoded =
                        binaryEncodedTypesList.getBinaryEncodedDataTypes().get(pos);
                    boolean sortOrderAsc = sortOrderList.getSortOrderAsc().get(pos++);
                    if (!isVarBinaryEncoded) {
                        while (keyOffset < keyBuffer.length && !isSeparatorByte(
                            keyBuffer[keyOffset++])) {
                            // empty
                        }
                    } else {
                        while (keyOffset < keyBuffer.length
                            && !SchemaUtil.areSeparatorBytesForVarBinaryEncoded(keyBuffer,
                            keyOffset++, sortOrderAsc ? SortOrder.ASC : SortOrder.DESC)) {
                            // empty
                        }
                        if (keyOffset < keyBuffer.length) {
                            keyOffset++;
                        }
                    }
                }
            }
        }
        return keyOffset;
    }

    public int getOffsetWithNullTypeAndOrderInfo(byte[] keyBuffer, int keyOffset) {
        // Use encoded offsets to navigate through row key buffer
        for (int offset : offsets) {
            if (offset >= 0) { // If offset is non negative, it's a byte offset
                keyOffset += offset;
            } else { // Else, a negative offset is the number of variable length values to skip
                while (offset++ < 0) {
                    // FIXME: keyOffset < keyBuffer.length required because HBase passes bogus keys to filter to position scan (HBASE-6562)
                    while (keyOffset < keyBuffer.length && !isSeparatorByte(
                        keyBuffer[keyOffset++])) {
                    }
                }
            }
        }
        return keyOffset;
    }

    /**
     * Calculate the length of the PK column value
     * @param keyBuffer the byte array of the row key
     * @param keyOffset the offset in the byte array of where the key begins
     * @param maxOffset maximum offset to use while calculating length
     * @param type the data type of the column.
     * @param sortOrder sort order.
     * @return the length of the PK column value
     */
    public int getLength(byte[] keyBuffer, int keyOffset, int maxOffset, PDataType type,
        SortOrder sortOrder) {
        if (!hasSeparator) {
            if (type == PVarbinaryEncoded.INSTANCE) {
                if (sortOrder == null || sortOrder == SortOrder.ASC) {
                    return maxOffset - keyOffset;
                } else if (sortOrder == SortOrder.DESC) {
                    return maxOffset - keyOffset - 2;
                }
            } else {
                return maxOffset - keyOffset - (
                    keyBuffer[maxOffset - 1] == QueryConstants.DESC_SEPARATOR_BYTE ? 1 : 0);
            }
        }
        int offset = keyOffset;
        if (type == PVarbinaryEncoded.INSTANCE) {
            while (offset < maxOffset && !SchemaUtil.areSeparatorBytesForVarBinaryEncoded(keyBuffer,
                offset, sortOrder)) {
                offset++;
            }
        } else {
            while (offset < maxOffset && !isSeparatorByte(keyBuffer[offset])) {
                offset++;
            }
        }
        return offset - keyOffset;
    }
}
