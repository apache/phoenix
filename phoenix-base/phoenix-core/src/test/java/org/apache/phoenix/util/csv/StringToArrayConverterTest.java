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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StringToArrayConverterTest extends BaseConnectionlessQueryTest {

    private Connection conn;
    private StringToArrayConverter converter;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection(getUrl());
        converter = new StringToArrayConverter(conn, ":", PVarchar.INSTANCE);
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void testToArray_EmptyString() throws SQLException {
        Array emptyArray = converter.toArray("");
        assertEquals(0, ((Object[]) emptyArray.getArray()).length);
    }


    @Test
    public void testToArray_SingleElement() throws SQLException {
        Array singleElementArray = converter.toArray("value");
        assertArrayEquals(
                new Object[]{"value"},
                (Object[]) singleElementArray.getArray());
    }

    @Test
    public void testToArray_MultipleElements() throws SQLException {
        Array multiElementArray = converter.toArray("one:two");
        assertArrayEquals(
                new Object[]{"one", "two"},
                (Object[]) multiElementArray.getArray());
    }

    @Test
    public void testToArray_IntegerValues() throws SQLException {
        StringToArrayConverter intArrayConverter = new StringToArrayConverter(
            conn, ":", PInteger.INSTANCE);
        Array intArray = intArrayConverter.toArray("1:2:3");
        assertArrayEquals(
                new int[]{1, 2, 3},
                (int[]) intArray.getArray());
    }
}
