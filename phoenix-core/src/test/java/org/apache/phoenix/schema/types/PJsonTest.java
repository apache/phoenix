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
package org.apache.phoenix.schema.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.json.PhoenixJsonTest;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

/**
 * Unit test for {@link PJson}.
 */
public class PJsonTest {

    final byte[] json = PhoenixJsonTest.TEST_JSON_STR.getBytes();

    @Test
    public void testToBytesWithOffset() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);

        byte[] bytes = new byte[json.length];

        assertEquals(json.length, PJson.INSTANCE.toBytes(phoenixJson, bytes, 0));

        assertArrayEquals(json, bytes);
    }

    @Test
    public void testToBytes() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);

        byte[] bytes = PJson.INSTANCE.toBytes(phoenixJson);
        assertArrayEquals(json, bytes);
    }
    
    @Test
    public void testToBytesForNull() throws Exception {
         assertEquals(ByteUtil.EMPTY_BYTE_ARRAY,PJson.INSTANCE.toBytes(null));
         assertEquals(ByteUtil.EMPTY_BYTE_ARRAY,PJson.INSTANCE.toBytes(null, SortOrder.ASC));
         assertEquals(ByteUtil.EMPTY_BYTE_ARRAY,PJson.INSTANCE.toBytes(null, SortOrder.DESC));
    }

    @Test
    public void testToObjectWithSortOrder() {
        Object object =
                PJson.INSTANCE.toObject(json, 0, json.length, PVarchar.INSTANCE, SortOrder.ASC,
                    Integer.MAX_VALUE, Integer.MAX_VALUE);
        PhoenixJson phoenixJson = (PhoenixJson) object;
        assertEquals(PhoenixJsonTest.TEST_JSON_STR, phoenixJson.toString());
    }

    @Test
    public void testToObject() {
        Object object =
                PJson.INSTANCE.toObject(json, 0, json.length, PVarchar.INSTANCE, SortOrder.ASC,
                    Integer.MAX_VALUE, Integer.MAX_VALUE);
        PhoenixJson phoenixJson = (PhoenixJson) object;
        assertEquals(PhoenixJsonTest.TEST_JSON_STR, phoenixJson.toString());

        Object object2 = PJson.INSTANCE.toObject(phoenixJson, PJson.INSTANCE);
        assertEquals(phoenixJson, object2);

        PJson.INSTANCE.toObject(PhoenixJsonTest.TEST_JSON_STR, PVarchar.INSTANCE);
        assertEquals(phoenixJson, object2);

        try {
            PJson.INSTANCE.toObject(PhoenixJsonTest.TEST_JSON_STR, PChar.INSTANCE);
        } catch (ConstraintViolationException sqe) {
            TypeMismatchException e = (TypeMismatchException) sqe.getCause();
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }

        PhoenixJson object4 = (PhoenixJson) PJson.INSTANCE.toObject(PhoenixJsonTest.TEST_JSON_STR);
        assertEquals(phoenixJson, object4);
        
    }

    @Test
    public void testToObjectForNull(){
        String jsonStr= null;
        assertNull(PJson.INSTANCE.toObject(jsonStr));
        
        Object jsonObj = null;
        assertNull(PJson.INSTANCE.toObject(jsonObj, PJson.INSTANCE));
    }

    @Test
    public void isFixedWidth() {
        assertFalse(PJson.INSTANCE.isFixedWidth());
    }

    public void getByteSize() {
        assertNull(PJson.INSTANCE.getByteSize());
    }

    public void estimateByteSize() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);
        assertEquals(PhoenixJsonTest.TEST_JSON_STR.length(),
            PJson.INSTANCE.estimateByteSize(phoenixJson));
    }

    @Test
    public void compareTo() throws Exception {
        PhoenixJson phoenixJson1 = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);

        PhoenixJson phoenixJson2 = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);

        assertEquals(0, PJson.INSTANCE.compareTo(phoenixJson1, phoenixJson2, PJson.INSTANCE));

        try {
            PJson.INSTANCE
                    .compareTo(phoenixJson1, PhoenixJsonTest.TEST_JSON_STR, PVarchar.INSTANCE);
        } catch (ConstraintViolationException cve) {
            TypeMismatchException e = (TypeMismatchException) cve.getCause();
            assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void isBytesComparableWith() {
        assertTrue(PJson.INSTANCE.isBytesComparableWith(PJson.INSTANCE));
        assertTrue(PJson.INSTANCE.isBytesComparableWith(PVarchar.INSTANCE));
        assertFalse(PJson.INSTANCE.isBytesComparableWith(PChar.INSTANCE));
    }

    @Test
    public void toStringLiteral() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);
        String stringLiteral = PJson.INSTANCE.toStringLiteral(phoenixJson, null);
        assertEquals(PVarchar.INSTANCE.toStringLiteral(PhoenixJsonTest.TEST_JSON_STR, null),
            stringLiteral);
    }

    @Test
    public void coerceBytes() throws SQLException {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(PhoenixJsonTest.TEST_JSON_STR);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(json, 0, 0);
        PJson.INSTANCE.coerceBytes(ptr, phoenixJson, PJson.INSTANCE, json.length, new Integer(10),
            SortOrder.ASC, json.length, new Integer(10), SortOrder.ASC);
        assertEquals(0, ptr.getLength());

        ptr.set(json);
        PJson.INSTANCE.coerceBytes(ptr, phoenixJson, PVarchar.INSTANCE, json.length,
            new Integer(10), SortOrder.ASC, json.length, new Integer(10), SortOrder.ASC);
        assertArrayEquals(json, ptr.get());
    }

}
