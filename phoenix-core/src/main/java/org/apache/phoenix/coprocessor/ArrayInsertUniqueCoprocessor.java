/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.phoenix.coprocessor.ArrayUpdateObserver.BASE_DATATYPE_ATTRIBUTE_SUFFIX;
import static org.apache.phoenix.coprocessor.ArrayUpdateObserver.FAMILY_ATTRIBUTE_SUFFIX;
import static org.apache.phoenix.coprocessor.ArrayUpdateObserver.QUALIFIER_ATTRIBUTE_SUFFIX;
import static org.apache.phoenix.coprocessor.ArrayUpdateObserver.SORTORDER_ATTRIBUTE_SUFFIX;
import org.apache.phoenix.schema.PArrayDataType;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.SortOrder;

public class ArrayInsertUniqueCoprocessor {

    ArrayInsertUniqueCoprocessor(ObserverContext<RegionCoprocessorEnvironment> env, Put put, String callerId) throws IOException {
        byte[] rowKey = put.getRow();

        byte[] columnFamilyName = put.getAttribute(callerId + FAMILY_ATTRIBUTE_SUFFIX);
        byte[] columnQualifierName = put.getAttribute(callerId + QUALIFIER_ATTRIBUTE_SUFFIX);
        PDataType baseDataType = PDataType.fromTypeId(Bytes.toInt(put.getAttribute(callerId + BASE_DATATYPE_ATTRIBUTE_SUFFIX)));
        SortOrder sortOrder = SortOrder.fromSystemValue(Bytes.toInt(put.getAttribute(callerId + SORTORDER_ATTRIBUTE_SUFFIX)));

        Get get = new Get(rowKey);
        get.addColumn(columnFamilyName, columnQualifierName);

        RegionCoprocessorEnvironment regionEnv = env.getEnvironment();

        HRegion region = regionEnv.getRegion();
        Result getResult = region.get(get);

        if (!getResult.isEmpty()) {
            byte[] currentValue = getResult.getValue(columnFamilyName, columnQualifierName);

            for (Cell cell : put.getFamilyCellMap().get(columnFamilyName)) {
                //get right cell
                if (Bytes.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
                        columnQualifierName, 0, columnQualifierName.length) == 0) {

                    ByteBuffer resultBuffer;
                    if (baseDataType.isFixedWidth()) {
                        resultBuffer = buildBufferFixedWidth(cell, baseDataType, currentValue);
                    } else {
                        try {
                            resultBuffer = buildBufferVarLength(cell, baseDataType, currentValue, sortOrder);
                        } catch (SQLException ex) {
                            throw new IOException(ex);
                        }
                    }
                    put.getFamilyCellMap().get(columnFamilyName).remove(cell);
                    put.add(keyValueFactory(rowKey, columnFamilyName, columnQualifierName,
                            resultBuffer.array(), resultBuffer.arrayOffset(), resultBuffer.position()));

                    env.complete();
                    return;
                }
            }
        }
        env.complete();
    }

    private KeyValue keyValueFactory(byte[] row, byte[] family, byte[] qualifier,
            byte[] valuesArray, int valuesOffset, int valuesLength) {
        return new KeyValue(
                row,
                0,
                row.length,
                family,
                0,
                family.length,
                qualifier,
                0,
                qualifier.length,
                HConstants.LATEST_TIMESTAMP,
                KeyValue.Type.Put,
                valuesArray,
                valuesOffset,
                valuesLength
        );

    }

    private boolean skipDuplicit(byte[] upsertValueArray, int upsertValueOffset,
            int itemIndex, int dataTypeLength, int upsertValueStartIndex) {

        for (int i = itemIndex; i > 0; i--) {
            int currentValueStartIndex = (i - 1) * dataTypeLength + upsertValueOffset;

            if (Bytes.compareTo(
                    upsertValueArray,
                    currentValueStartIndex,
                    dataTypeLength,
                    upsertValueArray,
                    upsertValueStartIndex,
                    dataTypeLength) == 0) {
                return true;
            }
        }
        return false;
    }

    private ByteBuffer buildBufferFixedWidth(Cell cell, PDataType baseDataType, byte[] currentValue) {
        byte[] upsertValueArray = cell.getValueArray();
        int upsertValueLength = cell.getValueLength();
        int upsertValueOffset = cell.getValueOffset();

        ByteBuffer resultBuffer = ByteBuffer.allocate(currentValue.length + upsertValueLength);
        resultBuffer.put(currentValue);

        int dataTypeLength = baseDataType.getByteSize();

        for (int i = 0; i < upsertValueLength / dataTypeLength; i++) {
            int upsertValueStartIndex = i * dataTypeLength + upsertValueOffset;
            if (skipDuplicit(upsertValueArray, upsertValueOffset, i, dataTypeLength, upsertValueStartIndex)) {
                break;
            }

            boolean allreadyExists = false;
            for (int j = 0; j < currentValue.length / dataTypeLength; j++) {
                int currentValueStartIndex = j * dataTypeLength;

                if (Bytes.compareTo(
                        currentValue,
                        currentValueStartIndex,
                        dataTypeLength,
                        upsertValueArray,
                        upsertValueStartIndex,
                        dataTypeLength) == 0) {
                    allreadyExists = true;
                    break;
                }
            }
            if (!allreadyExists) {
                resultBuffer.put(upsertValueArray, upsertValueStartIndex, dataTypeLength);
            }

        }
        return resultBuffer;
    }

    private ByteBuffer buildBufferVarLength(Cell cell, PDataType baseDataType,
            byte[] currentValue, SortOrder sortOrder) throws SQLException {

        byte[] upsertValueArray = cell.getValueArray();
        int upsertValueLength = cell.getValueLength();
        int upsertValueOffset = cell.getValueOffset();

        PDataType arrayPDataType = PDataType.fromTypeId(baseDataType.getSqlType() + PDataType.ARRAY_TYPE_BASE);
        TreeMap<byte[], Boolean> valuesMap = new TreeMap<byte[], Boolean>(new Bytes.ByteArrayComparator());
        //size of buffer: multiply by two is set for extreme case,
        //when each element has only one byte and we need space for delimiters
        ByteBuffer resultBuffer = ByteBuffer.allocate(currentValue.length + 2 * upsertValueLength);
        resultBuffer.put(currentValue, 0, currentValue.length - 2);

        PhoenixArray putArrayValues = (PhoenixArray) arrayPDataType
                .toObject(upsertValueArray, upsertValueOffset, upsertValueLength);
        PhoenixArray currentArrayValues = (PhoenixArray) arrayPDataType.toObject(currentValue);
        for (Object item : (Object[]) currentArrayValues.getArray()) {
            valuesMap.put(baseDataType.toBytes(item), (item == null));
        }

        List<Object> itemsToAppend = new ArrayList<Object>();

        //insert new values (tree map enforce uniqueness)
        for (Object item : (Object[]) putArrayValues.getArray()) {
            if (valuesMap.put(baseDataType.toBytes(item), (item == null)) == null) {
                itemsToAppend.add(item);
            }
        }

        PhoenixArray newArray = PArrayDataType.instantiatePhoenixArray(
                baseDataType, ArrayUtils.addAll((Object[]) currentArrayValues.getArray(), itemsToAppend.toArray()));

        byte[] newValue = arrayPDataType.toBytes(newArray, sortOrder);
        ByteBuffer byteBuffer = ByteBuffer.wrap(newValue);
        byteBuffer.position(newValue.length);
        return byteBuffer;
    }

}
