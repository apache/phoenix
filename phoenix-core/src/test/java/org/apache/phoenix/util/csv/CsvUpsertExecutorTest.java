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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CsvUpsertExecutorTest extends BaseConnectionlessQueryTest {

    private Connection conn;
    private List<ColumnInfo> columnInfoList;
    private PreparedStatement preparedStatement;
    private CsvUpsertExecutor.UpsertListener upsertListener;

    private CsvUpsertExecutor upsertExecutor;

    @Before
    public void setUp() throws SQLException {
        columnInfoList = ImmutableList.of(
                new ColumnInfo("ID", Types.BIGINT),
                new ColumnInfo("NAME", Types.VARCHAR),
                new ColumnInfo("AGE", Types.INTEGER),
                new ColumnInfo("VALUES", PIntegerArray.INSTANCE.getSqlType()));

        preparedStatement = mock(PreparedStatement.class);
        upsertListener = mock(CsvUpsertExecutor.UpsertListener.class);
        conn = DriverManager.getConnection(getUrl());
        upsertExecutor = new CsvUpsertExecutor(conn, columnInfoList, preparedStatement, upsertListener, ":");
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void testExecute() throws Exception {
        upsertExecutor.execute(createCsvRecord("123,NameValue,42,1:2:3"));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_TooFewFields() throws Exception {
        CSVRecord csvRecordWithTooFewFields = createCsvRecord("123,NameValue");
        upsertExecutor.execute(csvRecordWithTooFewFields);

        verify(upsertListener).errorOnRecord(eq(csvRecordWithTooFewFields), anyString());
        verifyNoMoreInteractions(upsertListener);
    }

    @Test
    public void testExecute_TooManyFields() throws Exception {
        CSVRecord csvRecordWithTooManyFields = createCsvRecord("123,NameValue,42,1:2:3,Garbage");
        upsertExecutor.execute(csvRecordWithTooManyFields);

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_NullField() throws Exception {
        upsertExecutor.execute(createCsvRecord("123,NameValue,,1:2:3"));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setNull(3, columnInfoList.get(2).getSqlType());
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_InvalidType() throws Exception {
        CSVRecord csvRecordWithInvalidType = createCsvRecord("123,NameValue,ThisIsNotANumber,1:2:3");
        upsertExecutor.execute(csvRecordWithInvalidType);

        verify(upsertListener).errorOnRecord(eq(csvRecordWithInvalidType), anyString());
        verifyNoMoreInteractions(upsertListener);
    }

    private CSVRecord createCsvRecord(String...columnValues) throws IOException {
        String inputRecord = Joiner.on(',').join(columnValues);
        return Iterables.getFirst(CSVParser.parse(inputRecord, CSVFormat.DEFAULT), null);
    }
}
