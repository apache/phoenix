/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;
import org.joda.time.DateTimeZone;

public class JodaTimezoneCache {

    public static final int CACHE_EXPRIRE_TIME_MINUTES = 10;
    private static final LoadingCache<ByteBuffer, DateTimeZone> cachedJodaTimeZones = createTimezoneCache();

    /**
     * Returns joda's DateTimeZone instance from cache or create new instance and cache it.
     *
     * @param timezoneId Timezone Id as accepted by {@code DateTimeZone.forID()}. E.g. Europe/Isle_of_Man
     * @return joda's DateTimeZone instance
     * @throws IllegalDataException if unknown timezone id is passed
     */
    public static DateTimeZone getInstance(ByteBuffer timezoneId) {
        try {
            return cachedJodaTimeZones.get(timezoneId);
        } catch (ExecutionException ex) {
            throw new IllegalDataException(ex);
        } catch (UncheckedExecutionException e) {
            throw new IllegalDataException("Unknown timezone " + Bytes.toString(timezoneId.array()));
        }
    }

    /**
     * Returns joda's DateTimeZone instance from cache or create new instance and cache it.
     *
     * @param timezoneId Timezone Id as accepted by {@code DateTimeZone.forID()}. E.g. Europe/Isle_of_Man
     * @return joda's DateTimeZone instance
     * @throws IllegalDataException if unknown timezone id is passed
     */
    public static DateTimeZone getInstance(ImmutableBytesWritable timezoneId) {
        return getInstance(ByteBuffer.wrap(timezoneId.copyBytes()));
    }

    /**
     * Returns joda's DateTimeZone instance from cache or create new instance and cache it.
     *
     * @param timezoneId Timezone Id as accepted by {@code DateTimeZone.forID()}. E.g. Europe/Isle_of_Man
     * @return joda's DateTimeZone instance
     * @throws IllegalDataException if unknown timezone id is passed
     */
    public static DateTimeZone getInstance(String timezoneId) {
        return getInstance(ByteBuffer.wrap(Bytes.toBytes(timezoneId)));
    }

    private static LoadingCache<ByteBuffer, DateTimeZone> createTimezoneCache() {
        return CacheBuilder.newBuilder().expireAfterAccess(CACHE_EXPRIRE_TIME_MINUTES, TimeUnit.MINUTES).build(new CacheLoader<ByteBuffer, DateTimeZone>() {

            @Override
            public DateTimeZone load(ByteBuffer timezone) throws Exception {
                return DateTimeZone.forID(Bytes.toString(timezone.array()));
            }
        });
    }

}
