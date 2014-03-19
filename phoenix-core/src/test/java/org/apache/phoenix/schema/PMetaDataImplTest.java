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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.phoenix.util.TimeKeeper;
import org.junit.Test;

import com.google.common.collect.Sets;

public class PMetaDataImplTest {
    
    private static void addToTable(PMetaData.Cache cache, String name, int size) {
        PTable table = new PSizedTable(new PTableKey(null,name), size);
        cache.put(table.getKey(), table);
    }
    
    private static PTable removeFromTable(PMetaData.Cache cache, String name) {
        return cache.remove(new PTableKey(null,name));
    }
    
    private static PTable getFromTable(PMetaData.Cache cache, String name) {
        return cache.get(new PTableKey(null,name));
    }
    
    private static void assertNames(PMetaData.Cache cache, String... names) {
        Set<String> actualTables = Sets.newHashSet();
        for (PTable table : cache) {
            actualTables.add(table.getKey().getName());
        }
        Set<String> expectedTables = Sets.newHashSet(names);
        assertEquals(expectedTables,actualTables);
    }
    
    private static class TestTimeKeeper implements TimeKeeper {
        private long time = 0;
        
        @Override
        public long getCurrentTime() {
            return time++;
        }
        
    }
    
    @Test
    public void testEviction() throws Exception {
        long maxSize = 10;
        PMetaData metaData = new PMetaDataImpl(5, maxSize, new TestTimeKeeper());
        PMetaData.Cache cache = metaData.getTables();
        addToTable(cache, "a", 5);
        assertEquals(1, cache.size());
        addToTable(cache, "b", 4);
        assertEquals(2, cache.size());
        addToTable(cache, "c", 3);
        assertEquals(2, cache.size());
        assertNames(cache, "b","c");

        addToTable(cache, "b", 8);
        assertEquals(1, cache.size());
        assertNames(cache, "b");

        addToTable(cache, "d", 11);
        assertEquals(1, cache.size());
        assertNames(cache, "d");
        
        removeFromTable(cache, "d");
        assertNames(cache);
        
        addToTable(cache, "a", 4);
        assertEquals(1, cache.size());
        addToTable(cache, "b", 3);
        assertEquals(2, cache.size());
        addToTable(cache, "c", 2);
        assertEquals(3, cache.size());
        assertNames(cache, "a", "b","c");
        
        getFromTable(cache, "a");
        addToTable(cache, "d", 3);
        assertEquals(3, cache.size());
        assertNames(cache, "c", "a","d");
        
        // Clone maintains insert order
        cache = cache.clone();
        addToTable(cache, "e", 6);
        assertEquals(2, cache.size());
        assertNames(cache, "d","e");
    }
    
    private static class PSizedTable extends PTableImpl {
        private final int size;
        private final PTableKey key;
        
        public PSizedTable(PTableKey key, int size) {
            this.key = key;
            this.size = size;
        }
        
        @Override
        public int getEstimatedSize() {
            return size;
        }
        
        @Override
        public PTableKey getKey() {
            return key;
        }
    }
}
