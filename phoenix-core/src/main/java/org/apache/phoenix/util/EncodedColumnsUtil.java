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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.StorageScheme;
import org.apache.phoenix.schema.types.PInteger;

public class EncodedColumnsUtil {

    public static boolean usesEncodedColumnNames(PTable table) {
        return table.getStorageScheme() != null && table.getStorageScheme() == StorageScheme.ENCODED_COLUMN_NAMES;
    }

    public static byte[] getEncodedColumnQualifier(PColumn column) {
        checkArgument(!SchemaUtil.isPKColumn(column), "No column qualifiers for PK columns");
        checkArgument(!column.isDynamic(), "No encoded column qualifiers for dynamic columns");
        return PInteger.INSTANCE.toBytes(column.getEncodedColumnQualifier());
    }

    public static byte[] getColumnQualifier(PColumn column, PTable table) {
      return EncodedColumnsUtil.getColumnQualifier(column, usesEncodedColumnNames(table));
    }

    public static byte[] getColumnQualifier(PColumn column, boolean encodedColumnName) {
        checkArgument(!SchemaUtil.isPKColumn(column), "No column qualifiers for PK columns");
        if (column.isDynamic()) { // Dynamic column names don't have encoded column names
            return column.getName().getBytes();
        }
        return encodedColumnName ? PInteger.INSTANCE.toBytes(column.getEncodedColumnQualifier()) : column.getName().getBytes(); 
    }

    /**
     * @return pair of byte arrays. The first part of the pair is the empty key value's column qualifier, and the second
     *         part is the value to use for it.
     */
    public static Pair<byte[], byte[]> getEmptyKeyValueInfo(PTable table) {
        return usesEncodedColumnNames(table) ? new Pair<>(QueryConstants.ENCODED_EMPTY_COLUMN_BYTES,
                QueryConstants.ENCODED_EMPTY_COLUMN_VALUE_BYTES) : new Pair<>(QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }

    /**
     * @return pair of byte arrays. The first part of the pair is the empty key value's column qualifier, and the second
     *         part is the value to use for it.
     */
    public static Pair<byte[], byte[]> getEmptyKeyValueInfo(boolean usesEncodedColumnNames) {
        return usesEncodedColumnNames ? new Pair<>(QueryConstants.ENCODED_EMPTY_COLUMN_BYTES,
                QueryConstants.ENCODED_EMPTY_COLUMN_VALUE_BYTES) : new Pair<>(QueryConstants.EMPTY_COLUMN_BYTES,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }

    public static boolean hasEncodedColumnName(PColumn column){
        return !SchemaUtil.isPKColumn(column) && !column.isDynamic() && column.getEncodedColumnQualifier() != null;
    }

}
