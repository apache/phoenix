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
import org.apache.phoenix.util.PhoenixKeyValueUtil;
import org.apache.phoenix.util.ViewIndexIdRetrieveUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;


import static org.apache.phoenix.coprocessorclient.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_INDEX_ID_DATA_TYPE_BYTES;
import static org.apache.phoenix.query.QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.NULL_DATA_TYPE_VALUE;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN;

public class SystemCatalogViewIndexIdFilter extends FilterBase implements Writable {
    private int clientVersion;

    public SystemCatalogViewIndexIdFilter() {
    }

    public SystemCatalogViewIndexIdFilter(int clientVersion) {
        this.clientVersion = clientVersion;
    }

    @Override
    public ReturnCode filterKeyValue(Cell keyValue) {
        return filterCell(keyValue);
    }

    @Override
    public ReturnCode filterCell(Cell keyValue) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
        Cell viewIndexIdCell = PhoenixKeyValueUtil.getColumnLatest(
                GenericKeyValueBuilder.INSTANCE, kvs,
                DEFAULT_COLUMN_FAMILY_BYTES, VIEW_INDEX_ID_BYTES);

        /*
            We retrieve the VIEW_INDEX_ID cell from SMALLINT to BIGINT or BIGINT to SMALLINT if and
            only if VIEW_INDEX_ID is included as part of the projected column.
            This is combination of diff client created view index looks like:
            client                  VIEW_INDEX_ID(Cell number of bytes)     VIEW_INDEX_ID_DATA_TYPE
        pre-4.15                        2 bytes                                     NULL
        post-4.15[config smallint]      2 bytes                                     5(smallint)
        post-4.15[config bigint]        8 bytes                                     -5(bigint)
         */
        if (viewIndexIdCell != null) {
            int type = NULL_DATA_TYPE_VALUE;
            Cell viewIndexIdDataTypeCell = PhoenixKeyValueUtil.getColumnLatest(
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
                /*
                    For pre-4.15 client select query cannot include VIEW_INDEX_ID_DATA_TYPE as part
                    of the projected columns; for this reason, the TYPE will always be NULL. Since
                    the pre-4.15 client always assume the VIEW_INDEX_ID column is type of SMALLINT,
                    we need to retrieve the BIGINT cell to SMALLINT cell.
                    VIEW_INDEX_ID_DATA_TYPE,      VIEW_INDEX_ID(Cell representation of the data)
                        NULL,                         SMALLINT         -> DO NOT CONVERT
                        SMALLINT,                     SMALLINT         -> DO NOT CONVERT
                        BIGINT,                       BIGINT           -> RETRIEVE AND SEND SMALLINT BACK
                 */
                if (type == NULL_DATA_TYPE_VALUE && viewIndexIdCell.getValueLength() >
                        VIEW_INDEX_ID_SMALLINT_TYPE_VALUE_LEN) {
                    Cell keyValue = ViewIndexIdRetrieveUtil.
                            getRetrievedViewIndexIdCell(viewIndexIdCell, false);
                    Collections.replaceAll(kvs, viewIndexIdCell, keyValue);
                }
            } else {
                /*
                    For post-4.15 client select query needs to include VIEW_INDEX_ID_DATA_TYPE as
                    part of the projected columns, and VIEW_INDEX_ID depends on it.
                    VIEW_INDEX_ID_DATA_TYPE,      VIEW_INDEX_ID(Cell representation of the data)
                        NULL,                         SMALLINT         -> RETRIEVE AND SEND BIGINT BACK
                        SMALLINT,                     SMALLINT         -> RETRIEVE AND SEND BIGINT BACK
                        BIGINT,                       BIGINT           -> DO NOT RETRIEVE
                 */
                if (type != Types.BIGINT && viewIndexIdCell.getValueLength() <
                        VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN) {
                    Cell keyValue = ViewIndexIdRetrieveUtil.
                            getRetrievedViewIndexIdCell(viewIndexIdCell, true);
                    Collections.replaceAll(kvs, viewIndexIdCell, keyValue);
                }
            }
        }
    }

    public static SystemCatalogViewIndexIdFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        try {
            SystemCatalogViewIndexIdFilter writable = (SystemCatalogViewIndexIdFilter)
                    Writables.getWritable(pbBytes, new SystemCatalogViewIndexIdFilter());
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