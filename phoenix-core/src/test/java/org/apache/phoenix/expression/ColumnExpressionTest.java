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
package org.apache.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

public class ColumnExpressionTest {

    @Test
    public void testSerialization() throws Exception {
        int maxLen = 30;
        int scale = 5;
        PColumn column = new PColumnImpl(PNameFactory.newName("c1"), PNameFactory.newName("f1"), PDataType.DECIMAL, maxLen, scale,
                true, 20, SortOrder.getDefault(), 0, null, false);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertEquals(maxLen, colExp2.getMaxLength().intValue());
        assertEquals(scale, colExp2.getScale().intValue());
        assertEquals(PDataType.DECIMAL, colExp2.getDataType());
    }

    @Test
    public void testSerializationWithNullScale() throws Exception {
        int maxLen = 30;
        PColumn column = new PColumnImpl(PNameFactory.newName("c1"), PNameFactory.newName("f1"), PDataType.BINARY, maxLen, null,
                true, 20, SortOrder.getDefault(), 0, null, false);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertEquals(maxLen, colExp2.getMaxLength().intValue());
        assertNull(colExp2.getScale());
        assertEquals(PDataType.BINARY, colExp2.getDataType());
    }

    @Test
    public void testSerializationWithNullMaxLength() throws Exception {
        int scale = 5;
        PColumn column = new PColumnImpl(PNameFactory.newName("c1"), PNameFactory.newName("f1"), PDataType.VARCHAR, null, scale,
                true, 20, SortOrder.getDefault(), 0, null, false);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertNull(colExp2.getMaxLength());
        assertEquals(scale, colExp2.getScale().intValue());
        assertEquals(PDataType.VARCHAR, colExp2.getDataType());
    }

    @Test
    public void testSerializationWithNullScaleAndMaxLength() throws Exception {
        PColumn column = new PColumnImpl(PNameFactory.newName("c1"), PNameFactory.newName("f1"), PDataType.DECIMAL, null, null, true,
                20, SortOrder.getDefault(), 0, null, false);
        ColumnExpression colExp = new KeyValueColumnExpression(column);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dOut = new DataOutputStream(baos);
        colExp.write(dOut);
        dOut.flush();

        ColumnExpression colExp2 = new KeyValueColumnExpression();
        byte[] bytes = baos.toByteArray();
        DataInputStream dIn = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length));
        colExp2.readFields(dIn);
        assertNull(colExp2.getMaxLength());
        assertNull(colExp2.getScale());
    }
}
