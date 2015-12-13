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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public abstract class AbstractUpsertExecutorTest<R, F> extends BaseConnectionlessQueryTest {

    protected Connection conn;
    protected List<ColumnInfo> columnInfoList;
    protected PreparedStatement preparedStatement;
    protected UpsertExecutor.UpsertListener<R> upsertListener;

    protected abstract UpsertExecutor<R, F> getUpsertExecutor();
    protected abstract R createRecord(Object... columnValues) throws IOException;

    @Before
    public void setUp() throws SQLException {
        columnInfoList = ImmutableList.of(
                new ColumnInfo("ID", Types.BIGINT),
                new ColumnInfo("NAME", Types.VARCHAR),
                new ColumnInfo("AGE", Types.INTEGER),
                new ColumnInfo("VALUES", PIntegerArray.INSTANCE.getSqlType()),
                new ColumnInfo("BEARD", Types.BOOLEAN));

        preparedStatement = mock(PreparedStatement.class);
        upsertListener = mock(UpsertExecutor.UpsertListener.class);
        conn = DriverManager.getConnection(getUrl());
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void testExecute() throws Exception {
        getUpsertExecutor().execute(createRecord(123L, "NameValue", 42,
                Arrays.asList(1, 2, 3), true));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.TRUE);
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_TooFewFields() throws Exception {
        R recordWithTooFewFields = createRecord(123L, "NameValue");
        getUpsertExecutor().execute(recordWithTooFewFields);

        verify(upsertListener).errorOnRecord(eq(recordWithTooFewFields), any(Throwable.class));
        verifyNoMoreInteractions(upsertListener);
    }

    @Test
    public void testExecute_TooManyFields() throws Exception {
        R recordWithTooManyFields = createRecord(123L, "NameValue", 42, Arrays.asList(1, 2, 3),
                true, "Garbage");
        getUpsertExecutor().execute(recordWithTooManyFields);

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.TRUE);
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_NullField() throws Exception {
        getUpsertExecutor().execute(createRecord(123L, "NameValue", null,
                Arrays.asList(1, 2, 3), false));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setNull(3, columnInfoList.get(2).getSqlType());
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.FALSE);
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_InvalidType() throws Exception {
        R recordWithInvalidType = createRecord(123L, "NameValue", "ThisIsNotANumber",
                Arrays.asList(1, 2, 3), true);
        getUpsertExecutor().execute(recordWithInvalidType);

        verify(upsertListener).errorOnRecord(eq(recordWithInvalidType), any(Throwable.class));
        verifyNoMoreInteractions(upsertListener);
    }
}
