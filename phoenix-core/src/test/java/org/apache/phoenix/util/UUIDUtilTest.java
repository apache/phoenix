/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import static org.junit.Assert.*;

import java.util.UUID;

import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

public class UUIDUtilTest {

    @Test
    public void testUUID() {
        String[] uuidsToTest = getUUIDsToTest();
        for (String temp : uuidsToTest) {
            byte[] bytes = UUIDUtil.getBytesFromUUID(UUID.fromString(temp));
            assertTrue(bytes.length == UUIDUtil.UUID_BYTES_LENGTH);

            // Test with SortOrder.ASC
            UUID uuid = UUIDUtil.getUUIDFromBytes(bytes, 0, SortOrder.ASC);
            assertTrue(uuid.equals(UUID.fromString(temp)));

            // Test with SortOrder.DESC
            byte[] bytesInverted = SortOrder.invert(bytes, 0, UUIDUtil.UUID_BYTES_LENGTH);
            assertTrue(bytesInverted.length == UUIDUtil.UUID_BYTES_LENGTH);
            uuid = UUIDUtil.getUUIDFromBytes(bytesInverted, 0, SortOrder.DESC);
            assertTrue(uuid.equals(UUID.fromString(temp)));
        }
    }

    @Test
    public void testUUIDIndexables() {
        String[] uuidsToTest = getUUIDsToTest();
        for (String temp : uuidsToTest) {
            byte[] bytes = UUIDUtil.getIndexablesBytesFromUUID(UUID.fromString(temp));
            assertTrue(bytes.length == UUIDUtil.UUID_BYTES_LENGTH_CODED);
            boolean noFFnoZeros = true;
            for (byte x : bytes) {
                if (x == (byte) 0x00 || x == (byte) 0xFF) {
                    noFFnoZeros = false;
                    break;
                }
            }
            assertTrue(noFFnoZeros);

            
            // Test with SortOrder.ASC
            UUID uuid = UUIDUtil.getUUIDFromIndexablesBytes(bytes, 0, bytes.length, SortOrder.ASC);
            assertTrue(uuid.equals(UUID.fromString(temp)));

            // Test with SortOrder.DESC
            byte[] bytesInverted = SortOrder.invert(bytes, 0, bytes.length);
            assertTrue(bytesInverted.length == UUIDUtil.UUID_BYTES_LENGTH_CODED);
            uuid =
                    UUIDUtil.getUUIDFromIndexablesBytes(bytesInverted, 0, bytesInverted.length,
                        SortOrder.DESC);
            assertTrue(uuid.equals(UUID.fromString(temp)));
        }
    }


    private String[] getUUIDsToTest() {

        String values[] =
                {       "00000000-0000-0000-0000-000000000000",
                        "ffffffff-ffff-ffff-ffff-ffffffffffff",

                        // some random UUIDS
                        "be05d4a5-86e2-40fb-adbe-c7ebf9917a93",
                        "7c649e6e-44c0-4150-bf4a-7f2d313bf9c0",
                        "b02766b0-6232-4eb1-a205-6e628a74f885",
                        "3a6bbee3-c87e-4f39-abb6-9f003d5900a7", // with a 0x00
                        "6b0b637c-41ed-4192-a90c-7b098976ff8b", // with a 0xff
                        "1d1fb1c8-3db5-413b-a32d-f40b5200ff75", // with a 0x00 and 0xff
                        "ebfdae7a-6676-4102-aee3-c5155ab3751a",
                        "06192856-a804-43d0-bc5f-2f08c61bc029",
                        "86d71708-c451-4c15-bff9-0b601cbd2fff", // ended with 0xff
                        "9d463c89-c8d2-4034-9492-ba9bdd8b7900", // ended with 0x00
                        "eb8647e6-c3a0-475c-9251-e36e3c3fba02",
                        "4f1ac4a6-17bf-4bd4-8374-808bd0c978d1",
                        "c5feff1f-d121-4202-9e6a-897910c12f9f",
                        "f32db84d-2491-4db4-974a-ac0be16decb7",
                        "a4cbcc1c-8419-40e3-938a-512e4f257ded" };
        return values;
    }

}
