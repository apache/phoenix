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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataTypeFactory;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

public class IndexUtilTest {
    
    @Test
    public void testIndexNonNullableColumnDataType() {
        verifyIndexColumnDataTypes(false, 
            "{BIGINT=BIGINT, "
             + "BIGINT ARRAY=BIGINT ARRAY, "
             + "BINARY=BINARY, "
             + "BINARY ARRAY=BINARY ARRAY, "
             + "BOOLEAN=BOOLEAN, "
             + "BOOLEAN ARRAY=BOOLEAN ARRAY, "
             + "BSON=BSON, "
             + "CHAR=CHAR, "
             + "CHAR ARRAY=CHAR ARRAY, "
             + "DATE=DATE, "
             + "DATE ARRAY=DATE ARRAY, "
             + "DECIMAL=DECIMAL, "
             + "DECIMAL ARRAY=DECIMAL ARRAY, "
             + "DOUBLE=DOUBLE, "
             + "DOUBLE ARRAY=DOUBLE ARRAY, "
             + "FLOAT=FLOAT, "
             + "FLOAT ARRAY=FLOAT ARRAY, "
             + "INTEGER=INTEGER, "
             + "INTEGER ARRAY=INTEGER ARRAY, "
             + "SMALLINT=SMALLINT, "
             + "SMALLINT ARRAY=SMALLINT ARRAY, "
             + "TIME=TIME, "
             + "TIME ARRAY=TIME ARRAY, "
             + "TIMESTAMP=TIMESTAMP, "
             + "TIMESTAMP ARRAY=TIMESTAMP ARRAY, "
             + "TINYINT=TINYINT, "
             + "TINYINT ARRAY=TINYINT ARRAY, "
             + "UNSIGNED_DATE=UNSIGNED_DATE, "
             + "UNSIGNED_DATE ARRAY=UNSIGNED_DATE ARRAY, "
             + "UNSIGNED_DOUBLE=UNSIGNED_DOUBLE, "
             + "UNSIGNED_DOUBLE ARRAY=UNSIGNED_DOUBLE ARRAY, "
             + "UNSIGNED_FLOAT=UNSIGNED_FLOAT, "
             + "UNSIGNED_FLOAT ARRAY=UNSIGNED_FLOAT ARRAY, "
             + "UNSIGNED_INT=UNSIGNED_INT, "
             + "UNSIGNED_INT ARRAY=UNSIGNED_INT ARRAY, "
             + "UNSIGNED_LONG=UNSIGNED_LONG, "
             + "UNSIGNED_LONG ARRAY=UNSIGNED_LONG ARRAY, "
             + "UNSIGNED_SMALLINT=UNSIGNED_SMALLINT, "
             + "UNSIGNED_SMALLINT ARRAY=UNSIGNED_SMALLINT ARRAY, "
             + "UNSIGNED_TIME=UNSIGNED_TIME, "
             + "UNSIGNED_TIME ARRAY=UNSIGNED_TIME ARRAY, "
             + "UNSIGNED_TIMESTAMP=UNSIGNED_TIMESTAMP, "
             + "UNSIGNED_TIMESTAMP ARRAY=UNSIGNED_TIMESTAMP ARRAY, "
             + "UNSIGNED_TINYINT=UNSIGNED_TINYINT, "
             + "UNSIGNED_TINYINT ARRAY=UNSIGNED_TINYINT ARRAY, "
             + "VARBINARY=VARBINARY, "
             + "VARBINARY ARRAY=VARBINARY ARRAY, "
             + "VARBINARY_ENCODED=VARBINARY_ENCODED, "
             + "VARCHAR=VARCHAR, "
             + "VARCHAR ARRAY=VARCHAR ARRAY}");
    }

    @Test
    public void testIndexNullableColumnDataType() {
        verifyIndexColumnDataTypes(true, 
            "{BIGINT=DECIMAL, "
             + "BIGINT ARRAY=BIGINT ARRAY, "
             + "BINARY=VARBINARY, "
             + "BINARY ARRAY=BINARY ARRAY, "
             + "BOOLEAN=DECIMAL, "
             + "BOOLEAN ARRAY=BOOLEAN ARRAY, "
             + "BSON=BSON, "
             + "CHAR=VARCHAR, "
             + "CHAR ARRAY=CHAR ARRAY, "
             + "DATE=DECIMAL, "
             + "DATE ARRAY=DATE ARRAY, "
             + "DECIMAL=DECIMAL, "
             + "DECIMAL ARRAY=DECIMAL ARRAY, "
             + "DOUBLE=DECIMAL, "
             + "DOUBLE ARRAY=DOUBLE ARRAY, "
             + "FLOAT=DECIMAL, "
             + "FLOAT ARRAY=FLOAT ARRAY, "
             + "INTEGER=DECIMAL, "
             + "INTEGER ARRAY=INTEGER ARRAY, "
             + "SMALLINT=DECIMAL, "
             + "SMALLINT ARRAY=SMALLINT ARRAY, "
             + "TIME=DECIMAL, "
             + "TIME ARRAY=TIME ARRAY, "
             + "TIMESTAMP=DECIMAL, "
             + "TIMESTAMP ARRAY=TIMESTAMP ARRAY, "
             + "TINYINT=DECIMAL, "
             + "TINYINT ARRAY=TINYINT ARRAY, "
             + "UNSIGNED_DATE=DECIMAL, "
             + "UNSIGNED_DATE ARRAY=UNSIGNED_DATE ARRAY, "
             + "UNSIGNED_DOUBLE=DECIMAL, "
             + "UNSIGNED_DOUBLE ARRAY=UNSIGNED_DOUBLE ARRAY, "
             + "UNSIGNED_FLOAT=DECIMAL, "
             + "UNSIGNED_FLOAT ARRAY=UNSIGNED_FLOAT ARRAY, "
             + "UNSIGNED_INT=DECIMAL, "
             + "UNSIGNED_INT ARRAY=UNSIGNED_INT ARRAY, "
             + "UNSIGNED_LONG=DECIMAL, "
             + "UNSIGNED_LONG ARRAY=UNSIGNED_LONG ARRAY, "
             + "UNSIGNED_SMALLINT=DECIMAL, "
             + "UNSIGNED_SMALLINT ARRAY=UNSIGNED_SMALLINT ARRAY, "
             + "UNSIGNED_TIME=DECIMAL, "
             + "UNSIGNED_TIME ARRAY=UNSIGNED_TIME ARRAY, "
             + "UNSIGNED_TIMESTAMP=DECIMAL, "
             + "UNSIGNED_TIMESTAMP ARRAY=UNSIGNED_TIMESTAMP ARRAY, "
             + "UNSIGNED_TINYINT=DECIMAL, "
             + "UNSIGNED_TINYINT ARRAY=UNSIGNED_TINYINT ARRAY, "
             + "VARBINARY=VARBINARY, "
             + "VARBINARY ARRAY=VARBINARY ARRAY, "
             + "VARBINARY_ENCODED=VARBINARY_ENCODED, "
             + "VARCHAR=VARCHAR, "
             + "VARCHAR ARRAY=VARCHAR ARRAY}");
    }

    private void verifyIndexColumnDataTypes(boolean isNullable, String expected) {
        Map<String, String> indexColumnDataTypes = Maps.newTreeMap();
        for (PDataType dataType : PDataTypeFactory.getInstance().getTypes()) {
            if (!dataType.isComparisonSupported()) {
                // JSON Datatype can't be an IndexColumn
                continue;
            }
            String indexColumnDataType = "unsupported";
            try {
                indexColumnDataType = IndexUtil.getIndexColumnDataType(isNullable, dataType).toString();
            } catch (IllegalArgumentException e) {
            }
            indexColumnDataTypes.put(dataType.toString(), indexColumnDataType);
        }
        assertEquals(expected, indexColumnDataTypes.toString());
    }
}
