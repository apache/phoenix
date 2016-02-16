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
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.util.TimeKeeper;
import org.junit.Test;

import com.google.common.collect.Sets;

public class PMetaDataImplTest {
    
    private static PMetaData addToTable(PMetaData metaData, String name, int size, TestTimeKeeper timeKeeper) throws SQLException {
        PTable table = new PSizedTable(new PTableKey(null,name), size);
        PMetaData newMetaData = metaData.addTable(table, System.currentTimeMillis());
        timeKeeper.incrementTime();
        return newMetaData;
    }
    
    private static PMetaData removeFromTable(PMetaData metaData, String name, TestTimeKeeper timeKeeper) throws SQLException {
        PMetaData newMetaData =  metaData.removeTable(null, name, null, HConstants.LATEST_TIMESTAMP);
        timeKeeper.incrementTime();
        return newMetaData;
    }
    
    private static PTable getFromTable(PMetaData metaData, String name, TestTimeKeeper timeKeeper) throws TableNotFoundException {
        PTable table = metaData.getTableRef(new PTableKey(null,name)).getTable();
        timeKeeper.incrementTime();
        return table;
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
            return time;
        }
        
        public void incrementTime() {
            time++;
        }
    }
    
    @Test
    public void testEviction() throws Exception {
        long maxSize = 10;
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        PMetaData metaData = new PMetaDataImpl(5, maxSize, timeKeeper);
        metaData = addToTable(metaData, "a", 5, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 4, timeKeeper);
        assertEquals(2, metaData.size());
        metaData = addToTable(metaData, "c", 3, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "b","c");

        metaData = addToTable(metaData, "b", 8, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "b");

        metaData = addToTable(metaData, "d", 11, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        
        metaData = removeFromTable(metaData, "d", timeKeeper);
        assertNames(metaData);
        
        metaData = addToTable(metaData, "a", 4, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 3, timeKeeper);
        assertEquals(2, metaData.size());
        metaData = addToTable(metaData, "c", 2, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b","c");
        
        getFromTable(metaData, "a", timeKeeper);
        metaData = addToTable(metaData, "d", 3, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "c", "a","d");
        
        // Clone maintains insert order
        metaData = metaData.clone();
        metaData = addToTable(metaData, "e", 6, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "d","e");
    }

    @Test
    public void shouldNotEvictMoreEntriesThanNecessary() throws Exception {
        long maxSize = 5;
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        PMetaData metaData = new PMetaDataImpl(5, maxSize, timeKeeper);
        metaData = addToTable(metaData, "a", 1, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 1, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "a", "b");
        metaData = addToTable(metaData, "c", 3, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b", "c");
        getFromTable(metaData, "a", timeKeeper);
        getFromTable(metaData, "b", timeKeeper);
        metaData = addToTable(metaData, "d", 3, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b", "d");
    }

    @Test
    public void shouldAlwaysKeepAtLeastOneEntryEvenIfTooLarge() throws Exception {
        long maxSize = 5;
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        PMetaData metaData = new PMetaDataImpl(5, maxSize, timeKeeper);
        metaData = addToTable(metaData, "a", 1, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 1, timeKeeper);
        assertEquals(2, metaData.size());
        metaData = addToTable(metaData, "c", 5, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "d", 20, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        metaData = addToTable(metaData, "e", 1, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "f", 2, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "e", "f");
    }

    @Test
    public void shouldAlwaysKeepOneEntryIfMaxSizeIsZero() throws Exception {
        long maxSize = 0;
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        PMetaData metaData = new PMetaDataImpl(0, maxSize, timeKeeper);
        metaData = addToTable(metaData, "a", 1, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "b", 1, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "c", 5, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "d", 20, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        metaData = addToTable(metaData, "e", 1, timeKeeper);
        assertEquals(1, metaData.size());
        metaData = addToTable(metaData, "f", 2, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "f");
    }

    @Test
    public void testAge() throws Exception {
        long maxSize = 10;
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        PMetaData metaData = new PMetaDataImpl(5, maxSize, timeKeeper);
        String tableName = "a";
        metaData = addToTable(metaData, tableName, 1, timeKeeper);
        PTableRef aTableRef = metaData.getTableRef(new PTableKey(null,tableName));
        assertNotNull(aTableRef);
        assertEquals(1, metaData.getAge(aTableRef));
        tableName = "b";
        metaData = addToTable(metaData, tableName, 1, timeKeeper);
        PTableRef bTableRef = metaData.getTableRef(new PTableKey(null,tableName));
        assertNotNull(bTableRef);
        assertEquals(1, metaData.getAge(bTableRef));
        assertEquals(2, metaData.getAge(aTableRef));
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
