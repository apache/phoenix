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
package org.apache.phoenix.util.csv;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.phoenix.schema.types.PDataType;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.phoenix.util.DateUtil;

/**
 * Converts strings with delimited values into Phoenix arrays.
 */
class StringToArrayConverter {

    private final Splitter splitter;
    private final Connection conn;
    private final PDataType elementDataType;
    private final CsvUpsertExecutor.SimpleDatatypeConversionFunction elementConvertFunction;

    /**
     * Instantiate with the array value separator and data type.
     *
     * @param conn Phoenix connection to target database
     * @param separatorString string used to separate incoming array values in strings
     * @param elementDataType datatype of the elements of arrays to be created
     */
    public StringToArrayConverter(Connection conn, String separatorString,
            PDataType elementDataType) {
        this.conn = conn;
        this.splitter = Splitter.on(separatorString);
        this.elementDataType = elementDataType;
        this.elementConvertFunction = new CsvUpsertExecutor.SimpleDatatypeConversionFunction(elementDataType, this.conn);
    }

    /**
     * Convert an input delimited string into a phoenix array of the configured type.
     *
     * @param input string containing delimited array values
     * @return the array containing the values represented in the input string
     */
    public Array toArray(String input) throws SQLException {
        if (input == null || input.isEmpty()) {
            return conn.createArrayOf(elementDataType.getSqlTypeName(), new Object[0]);
        }
        return conn.createArrayOf(
                elementDataType.getSqlTypeName(),
                Lists.newArrayList(
                        Iterables.transform(
                                splitter.split(input),
                                elementConvertFunction)).toArray());
    }
}
