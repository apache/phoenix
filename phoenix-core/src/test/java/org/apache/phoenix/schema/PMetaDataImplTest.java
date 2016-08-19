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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TimeKeeper;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class PMetaDataImplTest {
    
    private static void addToTable(PMetaData metaData, String name, int size, TestTimeKeeper timeKeeper) throws SQLException {
        PTable table = new PSizedTable(new PTableKey(null,name), size);
        metaData.addTable(table, System.currentTimeMillis());
        timeKeeper.incrementTime();
    }
    
    private static void removeFromTable(PMetaData metaData, String name, TestTimeKeeper timeKeeper) throws SQLException {
        metaData.removeTable(null, name, null, HConstants.LATEST_TIMESTAMP);
        timeKeeper.incrementTime();
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
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, "10");
        props.put(QueryServices.CLIENT_CACHE_ENCODING, "object");
        PMetaData metaData = new PMetaDataImpl(5, timeKeeper,  new ReadOnlyProps(props));
        addToTable(metaData, "a", 5, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "b", 4, timeKeeper);
        assertEquals(2, metaData.size());
        addToTable(metaData, "c", 3, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "b","c");

        addToTable(metaData, "b", 8, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "b");

        addToTable(metaData, "d", 11, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        
        removeFromTable(metaData, "d", timeKeeper);
        assertNames(metaData);
        
        addToTable(metaData, "a", 4, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "b", 3, timeKeeper);
        assertEquals(2, metaData.size());
        addToTable(metaData, "c", 2, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b","c");
        
        getFromTable(metaData, "a", timeKeeper);
        addToTable(metaData, "d", 3, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "c", "a","d");
        
        // Clone maintains insert order
        metaData = metaData.clone();
        addToTable(metaData, "e", 6, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "d","e");
    }

    @Test
    public void shouldNotEvictMoreEntriesThanNecessary() throws Exception {
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, "5");
        props.put(QueryServices.CLIENT_CACHE_ENCODING, "object");
        PMetaData metaData = new PMetaDataImpl(5, timeKeeper,  new ReadOnlyProps(props));
        addToTable(metaData, "a", 1, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "b", 1, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "a", "b");
        addToTable(metaData, "c", 3, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b", "c");
        getFromTable(metaData, "a", timeKeeper);
        getFromTable(metaData, "b", timeKeeper);
        addToTable(metaData, "d", 3, timeKeeper);
        assertEquals(3, metaData.size());
        assertNames(metaData, "a", "b", "d");
    }

    @Test
    public void shouldAlwaysKeepAtLeastOneEntryEvenIfTooLarge() throws Exception {
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, "5");
        props.put(QueryServices.CLIENT_CACHE_ENCODING, "object");
        PMetaData metaData = new PMetaDataImpl(5, timeKeeper,  new ReadOnlyProps(props));
        addToTable(metaData, "a", 1, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "b", 1, timeKeeper);
        assertEquals(2, metaData.size());
        addToTable(metaData, "c", 5, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "d", 20, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        addToTable(metaData, "e", 1, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "f", 2, timeKeeper);
        assertEquals(2, metaData.size());
        assertNames(metaData, "e", "f");
    }

    @Test
    public void shouldAlwaysKeepOneEntryIfMaxSizeIsZero() throws Exception {
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, "0");
        props.put(QueryServices.CLIENT_CACHE_ENCODING, "object");
        PMetaData metaData = new PMetaDataImpl(5, timeKeeper,  new ReadOnlyProps(props));
        addToTable(metaData, "a", 1, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "b", 1, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "c", 5, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "d", 20, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "d");
        addToTable(metaData, "e", 1, timeKeeper);
        assertEquals(1, metaData.size());
        addToTable(metaData, "f", 2, timeKeeper);
        assertEquals(1, metaData.size());
        assertNames(metaData, "f");
    }

    @Test
    public void testAge() throws Exception {
        TestTimeKeeper timeKeeper = new TestTimeKeeper();
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.MAX_CLIENT_METADATA_CACHE_SIZE_ATTRIB, "10");
        props.put(QueryServices.CLIENT_CACHE_ENCODING, "object");
        PMetaData metaData = new PMetaDataImpl(5, timeKeeper,  new ReadOnlyProps(props));
        String tableName = "a";
        addToTable(metaData, tableName, 1, timeKeeper);
        PTableRef aTableRef = metaData.getTableRef(new PTableKey(null,tableName));
        assertNotNull(aTableRef);
        assertEquals(1, metaData.getAge(aTableRef));
        tableName = "b";
        addToTable(metaData, tableName, 1, timeKeeper);
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
