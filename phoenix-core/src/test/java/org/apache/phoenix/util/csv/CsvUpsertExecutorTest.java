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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.util.AbstractUpsertExecutorTest;
import org.apache.phoenix.util.UpsertExecutor;
import org.junit.Before;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class CsvUpsertExecutorTest extends AbstractUpsertExecutorTest<CSVRecord, String> {

    private static final String ARRAY_SEP = ":";

    private UpsertExecutor<CSVRecord, String> upsertExecutor;

    @Override
    public UpsertExecutor<CSVRecord, String> getUpsertExecutor() {
        return upsertExecutor;
    }
    
    @Override
    public UpsertExecutor<CSVRecord, String> getUpsertExecutor(Connection conn) {
        return new CsvUpsertExecutor(conn, columnInfoList, preparedStatement,
                upsertListener, ARRAY_SEP);
    }
    
    @Override
    public CSVRecord createRecord(Object... columnValues) throws IOException {
        for (int i = 0; i < columnValues.length; i++) {
            if (columnValues[i] == null) {
                // Joiner.join throws on nulls, replace with empty string.
                columnValues[i] = "";
            }
            if (columnValues[i] instanceof List) {
                columnValues[i] = Joiner.on(ARRAY_SEP).join((List<?>) columnValues[i]);
            }
        }
        String inputRecord = Joiner.on(',').join(columnValues);
        return Iterables.getFirst(CSVParser.parse(inputRecord, CSVFormat.DEFAULT), null);
    }

    @Before
    public void setUp() throws SQLException {
        super.setUp();
        upsertExecutor = new CsvUpsertExecutor(conn, columnInfoList, preparedStatement,
                upsertListener, ARRAY_SEP);
    }

    
}
