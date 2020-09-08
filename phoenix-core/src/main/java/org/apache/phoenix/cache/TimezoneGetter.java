/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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
package org.apache.phoenix.cache;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.TimeZone;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;

public class TimezoneGetter {

    /**
     * Returns java's TimeZone instance from cache or create new instance and cache it.
     *
     * @param timezoneId Timezone Id as accepted by {@code ZoneId.of()}. E.g. Europe/Isle_of_Man
     * @return TimeZone
     * @throws IllegalDataException if unknown timezone id is passed
     */
    public static TimeZone getInstance(ByteBuffer timezoneId) {
        try {
            ZoneId zoneid = ZoneId.of(Bytes.toString(timezoneId.array()));
            return TimeZone.getTimeZone(zoneid);

        } catch (ZoneRulesException e) {
            throw new IllegalDataException("Unknown timezone " + Bytes
                    .toString(timezoneId.array()));
        }
    }

    /**
     * Returns java's TimeZone instance from cache or create new instance and cache it.
     *
     * @param timezoneId Timezone Id as accepted by {@code ZoneId.of()}. E.g. Europe/Isle_of_Man
     * @return TimeZone
     * @throws IllegalDataException if unknown timezone id is passed
     */
    public static TimeZone getInstance(ImmutableBytesWritable timezoneId) {
        return getInstance(ByteBuffer.wrap(timezoneId.copyBytes()));
    }

    /**
     * Returns java's TimeZone instance from cache or create new instance and cache it.
     *
     * @param timezoneId Timezone Id as accepted by {@code ZoneId.of()}. E.g. Europe/Isle_of_Man
     * @return TimeZone
     * @throws IllegalDataException if unknown timezone id is passed
     */
    public static TimeZone getInstance(String timezoneId) {
        return getInstance(ByteBuffer.wrap(Bytes.toBytes(timezoneId)));
    }
}
