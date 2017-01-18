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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public abstract class AbstractUpsertExecutorTest<R, F> extends BaseConnectionlessQueryTest {

    protected Connection conn;
    protected List<ColumnInfo> columnInfoList;
    protected PreparedStatement preparedStatement;
    protected UpsertExecutor.UpsertListener<R> upsertListener;

    protected abstract UpsertExecutor<R, F> getUpsertExecutor();
    protected abstract R createRecord(Object... columnValues) throws IOException;
    protected abstract UpsertExecutor<R, F> getUpsertExecutor(Connection conn);

    @Before
    public void setUp() throws SQLException {
        columnInfoList = ImmutableList.of(
                new ColumnInfo("ID", Types.BIGINT),
                new ColumnInfo("NAME", Types.VARCHAR),
                new ColumnInfo("AGE", Types.INTEGER),
                new ColumnInfo("VALUES", PIntegerArray.INSTANCE.getSqlType()),
                new ColumnInfo("BEARD", Types.BOOLEAN),
                new ColumnInfo("PIC", Types.BINARY));

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
        byte[] binaryData=(byte[])PBinary.INSTANCE.getSampleValue();
        String encodedBinaryData = Base64.encodeBytes(binaryData);
        getUpsertExecutor().execute(createRecord(123L, "NameValue", 42,
                Arrays.asList(1, 2, 3), true, encodedBinaryData));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.TRUE);
        verify(preparedStatement).setObject(6, binaryData);
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
        byte[] binaryData=(byte[])PBinary.INSTANCE.getSampleValue();
        String encodedBinaryData = Base64.encodeBytes(binaryData);
        R recordWithTooManyFields = createRecord(123L, "NameValue", 42, Arrays.asList(1, 2, 3),
                true, encodedBinaryData, "garbage");
        getUpsertExecutor().execute(recordWithTooManyFields);

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.TRUE);
        verify(preparedStatement).setObject(6, binaryData);
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_NullField() throws Exception {
        byte[] binaryData=(byte[])PBinary.INSTANCE.getSampleValue();
        String encodedBinaryData = Base64.encodeBytes(binaryData);
        getUpsertExecutor().execute(createRecord(123L, "NameValue", null,
                Arrays.asList(1, 2, 3), false, encodedBinaryData));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);

        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setNull(3, columnInfoList.get(2).getSqlType());
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.FALSE);
        verify(preparedStatement).setObject(6, binaryData);
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    @Test
    public void testExecute_InvalidType() throws Exception {
        byte[] binaryData=(byte[])PBinary.INSTANCE.getSampleValue();
        String encodedBinaryData = Base64.encodeBytes(binaryData);
        R recordWithInvalidType = createRecord(123L, "NameValue", "ThisIsNotANumber",
                Arrays.asList(1, 2, 3), true, encodedBinaryData);
        getUpsertExecutor().execute(recordWithInvalidType);

        verify(upsertListener).errorOnRecord(eq(recordWithInvalidType), any(Throwable.class));
        verifyNoMoreInteractions(upsertListener);
    }
    
    @Test
    public void testExecute_InvalidBoolean() throws Exception {
        byte[] binaryData=(byte[])PBinary.INSTANCE.getSampleValue();
        String encodedBinaryData = Base64.encodeBytes(binaryData);
        R csvRecordWithInvalidType = createRecord("123,NameValue,42,1:2:3,NotABoolean,"+encodedBinaryData);
        getUpsertExecutor().execute(csvRecordWithInvalidType);

        verify(upsertListener).errorOnRecord(eq(csvRecordWithInvalidType), any(Throwable.class));
    }
    
    @Test
    public void testExecute_InvalidBinary() throws Exception {
        String notBase64Encoded="#@$df";
        R csvRecordWithInvalidType = createRecord("123,NameValue,42,1:2:3,true,"+notBase64Encoded);
        getUpsertExecutor().execute(csvRecordWithInvalidType);

        verify(upsertListener).errorOnRecord(eq(csvRecordWithInvalidType), any(Throwable.class));
    }
    
    @Test
    public void testExecute_AsciiEncoded() throws Exception {
        String asciiValue="#@$df";
        Properties info=new Properties();
        info.setProperty(QueryServices.UPLOAD_BINARY_DATA_TYPE_ENCODING,"ASCII");
        getUpsertExecutor(DriverManager.getConnection(getUrl(),info)).execute(createRecord(123L, "NameValue", 42,
                Arrays.asList(1, 2, 3), true, asciiValue));

        verify(upsertListener).upsertDone(1L);
        verifyNoMoreInteractions(upsertListener);
        
        verify(preparedStatement).setObject(1, Long.valueOf(123L));
        verify(preparedStatement).setObject(2, "NameValue");
        verify(preparedStatement).setObject(3, Integer.valueOf(42));
        verify(preparedStatement).setObject(4, PArrayDataType.instantiatePhoenixArray(PInteger.INSTANCE, new Object[]{1,2,3}));
        verify(preparedStatement).setObject(5, Boolean.TRUE);
        verify(preparedStatement).setObject(6, Bytes.toBytes(asciiValue));
        verify(preparedStatement).execute();
        verifyNoMoreInteractions(preparedStatement);
    }

    
}
