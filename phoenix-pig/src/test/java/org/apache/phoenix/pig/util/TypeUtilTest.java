/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pig.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.phoenix.pig.writable.PhoenixPigDBWritable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TypeUtilTest {

    @Test
    public void testTransformToTuple() throws Exception {
        PhoenixPigDBWritable record = mock(PhoenixPigDBWritable.class);
        List<Object> values = Lists.newArrayList();
        values.add("213123");
        values.add(1231123);
        values.add(31231231232131L);
        values.add("bytearray".getBytes());
        when(record.getValues()).thenReturn(values);

        ResourceFieldSchema field = new ResourceFieldSchema().setType(DataType.CHARARRAY);
        ResourceFieldSchema field1 = new ResourceFieldSchema().setType(DataType.INTEGER);
        ResourceFieldSchema field2 = new ResourceFieldSchema().setType(DataType.LONG);
        ResourceFieldSchema field3 = new ResourceFieldSchema().setType(DataType.BYTEARRAY);
        ResourceFieldSchema[] projectedColumns = { field, field1, field2, field3 };

        Tuple t = TypeUtil.transformToTuple(record, projectedColumns);

        assertEquals(DataType.LONG, DataType.findType(t.get(2)));

        field = new ResourceFieldSchema().setType(DataType.BIGDECIMAL);
        field1 = new ResourceFieldSchema().setType(DataType.BIGINTEGER);
        values.clear();
        values.add(new BigDecimal(123123123.123213));
        values.add(new BigInteger("1312313231312"));
        ResourceFieldSchema[] columns = { field, field1 };
        t = TypeUtil.transformToTuple(record, columns);

        assertEquals(DataType.BIGDECIMAL, DataType.findType(t.get(0)));
        assertEquals(DataType.BIGINTEGER, DataType.findType(t.get(1)));
    }
}
