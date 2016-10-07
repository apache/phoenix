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
package org.apache.phoenix.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.query.GuidePostsCache.PhoenixStatsCacheRemovalListener;
import org.junit.Test;

import com.google.common.cache.RemovalCause;

/**
 * Test class around the PhoenixStatsCacheRemovalListener.
 */
public class PhoenixStatsCacheRemovalListenerTest {

    @Test
    public void nonEvictionsAreIgnored() {
        // We don't care so much about cases where we trigger a removal or update of the stats
        // for a table in the cache, but we would want to know about updates happening automatically
        PhoenixStatsCacheRemovalListener listener = new PhoenixStatsCacheRemovalListener();
        // User-driven removals or updates
        assertFalse(listener.wasEvicted(RemovalCause.EXPLICIT));
        assertFalse(listener.wasEvicted(RemovalCause.REPLACED));
        // Automatic removals by the cache itself (per configuration)
        assertTrue(listener.wasEvicted(RemovalCause.COLLECTED));
        assertTrue(listener.wasEvicted(RemovalCause.EXPIRED));
        assertTrue(listener.wasEvicted(RemovalCause.SIZE));
    }
}