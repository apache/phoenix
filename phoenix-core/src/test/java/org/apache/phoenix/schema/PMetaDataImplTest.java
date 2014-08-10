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

import java.sql.SQLException;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.util.TimeKeeper;
import org.junit.Test;

import com.google.common.collect.Sets;

public class PMetaDataImplTest {
    
    private static PMetaData addToTable(PMetaData metaData, String name, int size) throws SQLException {
        PTable table = new PSizedTable(new PTableKey(null,name), size);
        return metaData.addTable(table);
    }
    
    private static PMetaData removeFromTable(PMetaData metaData, String name) throws SQLException {
        return metaData.removeTable(null, name, null, HConstants.LATEST_TIMESTAMP);
    }
    
    private static PTable getFromTable(PMetaData metaData, String name) throws TableNotFoundException {
        return metaData.getTable(new PTableKey(null,name));
    }
    
    private static void assertNames(PMetaData metaData, String... names) {
        Set<String> actualTables = Sets.newHashSet();
        for (PTable table : metaData) {
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
        metaData = addToTable(metaData, "a", 5);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 4);
        assertEquals(2, metaData.size());
        metaData = addToTable(metaData, "c", 3);
        assertEquals(2, metaData.size());
        assertNames(metaData, "b","c");

        metaData = addToTable(metaData, "b", 8);
        assertEquals(1, metaData.size());
        assertNames(metaData, "b");

        metaData = addToTable(metaData, "d", 11);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        
        metaData = removeFromTable(metaData, "d");
        assertNames(metaData);
        
        metaData = addToTable(metaData, "a", 4);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 3);
        assertEquals(2, metaData.size());
        metaData = addToTable(metaData, "c", 2);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b","c");
        
        getFromTable(metaData, "a");
        metaData = addToTable(metaData, "d", 3);
        assertEquals(3, metaData.size());
        assertNames(metaData, "c", "a","d");
        
        // Clone maintains insert order
        metaData = metaData.clone();
        metaData = addToTable(metaData, "e", 6);
        assertEquals(2, metaData.size());
        assertNames(metaData, "d","e");
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
