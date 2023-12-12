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
package org.apache.phoenix.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.SortOrder;

/**
 * UUID util class. It contains the algorithm that converts from UUID to bytes and from bytes to
 * UUID. Depending on whether the UUID is indexable or not, some methods or others are called.
 */
public class UUIDUtil {

    private UUIDUtil() {
    }

    public static final int UUID_BYTES_LENGTH = 16;

    // "f90b6b50-f1b5-459b-b4dd-d6da4ddb5655".length() == 36
    public static final int UUID_BYTES_LENGTH_CODED = 36;

    public static byte[] getBytesFromUUID(UUID uuid) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[UUID_BYTES_LENGTH]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return byteBuffer.array();
    }

    public static UUID getUUIDFromBytes(byte[] bytes, int offset, SortOrder sortOrder) {

        ByteBuffer byteBuffer;
        if (sortOrder == null || sortOrder == SortOrder.ASC) {
            byteBuffer = ByteBuffer.wrap(bytes, offset, UUID_BYTES_LENGTH);
        } else {
            byteBuffer =
                    ByteBuffer.wrap(SortOrder.invert(bytes, offset, new byte[UUID_BYTES_LENGTH], 0,
                        UUID_BYTES_LENGTH), 0, UUID_BYTES_LENGTH);
        }
        Long high = byteBuffer.getLong();
        Long low = byteBuffer.getLong();

        return new UUID(high, low);
    }

    public static byte[] getIndexablesBytesFromUUID(UUID uuid) {
        return uuid.toString().getBytes(Charset.forName("UTF-8"));

    }

    public static UUID getUUIDFromIndexablesBytes(byte[] bytes, int offset, int length,
            SortOrder sortOrder) {

        if (length == UUID_BYTES_LENGTH_CODED) {

            String uuidStr;
            if (sortOrder == null || sortOrder == SortOrder.ASC) {
                uuidStr =
                        new String(bytes, offset, UUID_BYTES_LENGTH_CODED,
                                Charset.forName("UTF-8"));
            } else {
                byte[] temp =
                        SortOrder.invert(bytes, offset, new byte[UUID_BYTES_LENGTH_CODED], 0,
                            UUID_BYTES_LENGTH_CODED);
                uuidStr = new String(temp, Charset.forName("UTF-8"));
            }

            try {
                return UUID.fromString(uuidStr);
            } catch (IllegalArgumentException e) {
                throw new ConstraintViolationException(
                        "Bytes are malformed as indexable bytes to UUID : \"" + uuidStr + "\"");
            }
        } else {
            throw new ConstraintViolationException(
                    "Bytes are malformed as indexable bytes to UUID");
        }
    }

}
