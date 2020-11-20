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
package org.apache.phoenix.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.KeyValueUtil;
import org.apache.phoenix.util.ViewIndexIdRetrieveUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;


import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE_BYTES;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.NULL_DATA_TYPE_VALUE;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN;

public class SyscatViewIndexIdFilter extends FilterBase implements Writable {
    private int clientVersion;

    public SyscatViewIndexIdFilter() {
    }

    public SyscatViewIndexIdFilter(int clientVersion) {
        this.clientVersion = clientVersion;
    }

    @Override
    public ReturnCode filterKeyValue(Cell keyValue) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        Cell viewIndexIdCell = KeyValueUtil.getColumnLatest(
                GenericKeyValueBuilder.INSTANCE, kvs,
                DEFAULT_COLUMN_FAMILY_BYTES, VIEW_INDEX_ID_BYTES);

        if (viewIndexIdCell != null) {
            int type = NULL_DATA_TYPE_VALUE;
            Cell viewIndexIdDataTypeCell = KeyValueUtil.getColumnLatest(
                    GenericKeyValueBuilder.INSTANCE, kvs,
                    DEFAULT_COLUMN_FAMILY_BYTES, VIEW_INDEX_ID_DATA_TYPE_BYTES);
            if (viewIndexIdDataTypeCell != null) {
                type = (Integer) PInteger.INSTANCE.toObject(
                        viewIndexIdDataTypeCell.getValueArray(),
                        viewIndexIdDataTypeCell.getValueOffset(),
                        viewIndexIdDataTypeCell.getValueLength(),
                        PInteger.INSTANCE,
                        SortOrder.ASC);
            }
            if (this.clientVersion < MIN_SPLITTABLE_SYSTEM_CATALOG) {
                // pre-splittable client should always using SMALLINT
                if (type == NULL_DATA_TYPE_VALUE && viewIndexIdCell.getValueLength() >
                        VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN) {
                    Cell keyValue = ViewIndexIdRetrieveUtil.
                            getViewIndexIdKeyValueInShortDataFormat(viewIndexIdCell);
                    Collections.replaceAll(kvs, viewIndexIdCell, keyValue);
                }
            } else {
                // post-splittable client should always using BIGINT
                if (type != Types.BIGINT && viewIndexIdCell.getValueLength() <
                        VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN) {
                    Cell keyValue = ViewIndexIdRetrieveUtil.
                            getViewIndexIdKeyValueInLongDataFormat(viewIndexIdCell);
                    Collections.replaceAll(kvs, viewIndexIdCell, keyValue);
                }
            }
        }
    }

    public static SyscatViewIndexIdFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            SyscatViewIndexIdFilter writable = (SyscatViewIndexIdFilter)
                    Writables.getWritable(pbBytes, new SyscatViewIndexIdFilter());
            return writable;
        } catch (IOException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Writables.getBytes(this);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.clientVersion = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(this.clientVersion);
    }

}
