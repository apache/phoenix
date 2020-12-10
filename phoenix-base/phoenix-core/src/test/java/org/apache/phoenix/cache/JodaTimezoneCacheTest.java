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
 */
package org.apache.phoenix.cache;

import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;
import org.joda.time.DateTimeZone;
import org.junit.Test;

public class JodaTimezoneCacheTest {

    @Test
    public void testGetInstanceByteBufferUTC() {
        DateTimeZone instance = JodaTimezoneCache.getInstance(ByteBuffer.wrap(Bytes.toBytes("UTC")));
        assertNotNull(instance);
    }

    @Test
    public void testGetInstanceString() {
        DateTimeZone instance = JodaTimezoneCache.getInstance("America/St_Vincent");
        assertNotNull(instance);
    }

    @Test(expected = IllegalDataException.class)
    public void testGetInstanceStringUnknown() {
        JodaTimezoneCache.getInstance("SOME_UNKNOWN_TIMEZONE");
    }

    @Test
    public void testGetInstanceImmutableBytesWritable() {
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(Bytes.toBytes("Europe/Isle_of_Man"));
        DateTimeZone instance = JodaTimezoneCache.getInstance(ptr);
        assertNotNull(instance);
    }
}
