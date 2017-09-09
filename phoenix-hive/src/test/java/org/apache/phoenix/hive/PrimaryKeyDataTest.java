/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

import org.junit.Test;

public class PrimaryKeyDataTest {
    private static class Disallowed implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
        }
        return baos.toByteArray();
    }

    @Test
    public void testSerde() throws Exception {
        HashMap<String,Object> data = new HashMap<>();
        data.put("one", 1);
        data.put("two", "two");
        data.put("three", 3);

        PrimaryKeyData pkData = new PrimaryKeyData(data);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pkData.serialize(baos);

        PrimaryKeyData pkCopy = PrimaryKeyData.deserialize(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(data, pkCopy.getData());
    }

    @Test
    public void testDisallowedDeserialization() throws Exception {
        byte[] serializedMap = serialize(new HashMap<String,Object>());
        byte[] serializedClass = serialize(new Disallowed());
        byte[] serializedString = serialize("asdf");

        try {
            PrimaryKeyData.deserialize(new ByteArrayInputStream(serializedMap));
            fail("Expected an InvalidClassException");
        } catch (InvalidClassException e) {}
        try {
            PrimaryKeyData.deserialize(new ByteArrayInputStream(serializedClass));
            fail("Expected an InvalidClassException");
        } catch (InvalidClassException e) {}
        try {
            PrimaryKeyData.deserialize(new ByteArrayInputStream(serializedString));
            fail("Expected an InvalidClassException");
        } catch (InvalidClassException e) {}
    }
}
