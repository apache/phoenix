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
package org.apache.phoenix.util;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.RowKeyMatcher;
import org.apache.phoenix.coprocessorclient.TableTTLInfo;
import org.apache.phoenix.coprocessorclient.TableTTLInfoCache;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RowKeyMatcherTest {

    public List<TableTTLInfo> getSampleData() {

        List<TableTTLInfo> tableList = new ArrayList<TableTTLInfo>();
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000001", "001", 30000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000002", "002", 60000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000003", "003", 60000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0001000001", "TEST_ENTITY.Z01", "00D0t0001000001Z01", 60000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0002000001", "TEST_ENTITY.Z01","00D0t0002000001Z01", 120000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0003000001", "TEST_ENTITY.Z01","00D0t0003000001Z01", 180000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0004000001", "TEST_ENTITY.Z01","00D0t0004000001Z01", 300000));
        tableList.add(new TableTTLInfo("TEST_ENTITY.T_000001", "00D0t0005000001", "TEST_ENTITY.Z01","00D0t0005000001Z01", 6000));
        return tableList;
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testOverlappingPrefixes() {
        RowKeyMatcher globalRowKeyMatcher = new RowKeyMatcher();
        RowKeyMatcher tenantRowKeyMatcher = new RowKeyMatcher();
        TableTTLInfoCache cache = new TableTTLInfoCache();
        List<String> sampleRows = new ArrayList<>();
        sampleRows.add("0010t0001000001001Z01#12348");
        sampleRows.add("0010t0001000001002Z01#7832438");

        TableTTLInfo table1 = new TableTTLInfo("TEST_ENTITY.T_000001", "", "TEST_ENTITY.GV_000001", "001", 30000);
        TableTTLInfo table2 = new TableTTLInfo("TEST_ENTITY.T_000001",
                "0010t0001000001", "TEST_ENTITY.Z01",
                "0010t0001000001002Z01", 60000);

        Integer tableId1 = cache.addTable(table1);
        Integer tableId2 = cache.addTable(table2);
        globalRowKeyMatcher.put(table1.getMatchPattern(), tableId1);
        tenantRowKeyMatcher.put(table2.getMatchPattern(), tableId2);

        int tenantOffset = 0;
        int globalOffset = 15;

        Integer row0GlobalMatch = globalRowKeyMatcher.get(sampleRows.get(0).getBytes(), globalOffset);
        assertTrue(String.format("row-%d, matched = %s, row = %s",
                0, row0GlobalMatch != null, sampleRows.get(0)), row0GlobalMatch != null);
        Integer row0TenantMatch = tenantRowKeyMatcher.get(sampleRows.get(0).getBytes(), tenantOffset);
        assertTrue(String.format("row-%d, matched = %s, row = %s",
                0, row0TenantMatch != null, sampleRows.get(0)), row0TenantMatch == null);

        Integer row1GlobalMatch = globalRowKeyMatcher.get(sampleRows.get(1).getBytes(), globalOffset);
        assertTrue(String.format("row-%d, matched = %s, row = %s",
                0, row1GlobalMatch != null, sampleRows.get(1)), row1GlobalMatch == null);
        Integer row1TenantMatch = tenantRowKeyMatcher.get(sampleRows.get(1).getBytes(), tenantOffset);
        assertTrue(String.format("row-%d, matched = %s, row = %s",
                0, row1TenantMatch != null, sampleRows.get(1)), row1TenantMatch != null);
    }

    @Test
    public void testSamplePrefixes() {
        RowKeyMatcher rowKeyMatcher = new RowKeyMatcher();
        TableTTLInfoCache cache = new TableTTLInfoCache();
        List<TableTTLInfo> sampleTableList = new ArrayList<TableTTLInfo>();

        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000001".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x00"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000002".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x01"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000003".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x02"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000004".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x03"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000005".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x04"),
                300));

        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000006".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x8F"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000007".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\x9F"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000008".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\xAF"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000009".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\xBF"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_0000010".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\xCF"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000011".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\xDF"),
                300));
        sampleTableList.add(new TableTTLInfo("TEST_ENTITY.T_000001".getBytes(),
                ByteUtil.EMPTY_BYTE_ARRAY, "TEST_ENTITY.GV_000012".getBytes(),
                Bytes.toBytesBinary("\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\x80\\xEF"),
                300));


        for (int i = 0; i < sampleTableList.size(); i++) {
            Integer tableId = cache.addTable(sampleTableList.get(i));
            rowKeyMatcher.put(sampleTableList.get(i).getMatchPattern(), tableId);
            assertEquals(tableId.intValue(), i);
        }
        int offset = 0;
        for (int i = 0; i<sampleTableList.size(); i++) {
            assertEquals(rowKeyMatcher.get(sampleTableList.get(i).getMatchPattern(), offset).intValue(), i);
        }

    }

    @Test
    public void testSingleSampleDataCount() {
        RowKeyMatcher rowKeyMatcher = new RowKeyMatcher();
        TableTTLInfoCache cache = new TableTTLInfoCache();

        List<TableTTLInfo> sampleTableList = getSampleData();
        runTest(rowKeyMatcher, cache, sampleTableList, 1, 1);

        //Assert results
        assertResults(rowKeyMatcher, sampleTableList);
        assertResults(cache, sampleTableList);

    }

    @Test
    public void testRepeatingSampleDataCount() {
        RowKeyMatcher rowKeyMatcher = new RowKeyMatcher();
        TableTTLInfoCache cache = new TableTTLInfoCache();
        List<TableTTLInfo> sampleTableList = getSampleData();
        runTest(rowKeyMatcher, cache, sampleTableList, 1, 25);

        //Assert results
        assertResults(rowKeyMatcher, sampleTableList);
        assertResults(cache, sampleTableList);

    }

    @Test
    public void testConcurrentSampleDataCount() {
        RowKeyMatcher rowKeyMatcher = new RowKeyMatcher();
        TableTTLInfoCache cache = new TableTTLInfoCache();
        List<TableTTLInfo> sampleTableList = getSampleData();
        runTest(rowKeyMatcher, cache, sampleTableList, 5, 5);

        //Assert results
        assertResults(rowKeyMatcher, sampleTableList);
        assertResults(cache, sampleTableList);
    }

    private void assertResults(RowKeyMatcher rowKeyMatcher, List<TableTTLInfo> sampleTableList) {
        //Assert results
        int tableCountExpected = sampleTableList.size();
        int prefixCountActual = rowKeyMatcher.getNumEntries();
        String message = String.format("expected = %d, actual = %d", tableCountExpected, prefixCountActual);
        assertTrue(message, tableCountExpected == prefixCountActual);
    }

    private void assertResults(TableTTLInfoCache cache, List<TableTTLInfo> sampleTableList) {
//        //Assert results
        Set<TableTTLInfo> dedupedTables = new HashSet<TableTTLInfo>();
        dedupedTables.addAll(sampleTableList);

        int tableCountExpected = sampleTableList.size();
        int dedupeTableCountExpected = dedupedTables.size();
        int tableCountActual = cache.getNumTablesInCache();
        String message = String.format("expected = %d, actual = %d", tableCountExpected, tableCountActual);
        assertTrue(message, dedupeTableCountExpected == tableCountActual);
        assertTrue(message, tableCountExpected == tableCountActual);
    }

    private void runTest(RowKeyMatcher targetRowKeyMatcher, TableTTLInfoCache cache,
            List<TableTTLInfo> sampleData, int numThreads, int numRepeats) {

        try {
            Thread[] threads = new Thread[numThreads];
            final CountDownLatch latch = new CountDownLatch(threads.length);
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(new Runnable() {
                    public void run() {
                        try {
                            for (int repeats = 0; repeats < numRepeats; repeats++) {
                                addTablesToPrefixIndex(sampleData, targetRowKeyMatcher);
                                addTablesToCache(sampleData, cache);
                            }
                        } finally {
                            latch.countDown();
                        }
                    }
                }, "data-generator-" + i);
                threads[i].setDaemon(true);
            }
            for (int i = 0; i < threads.length; i++) {
                threads[i].start();
            }
            latch.await();
        }
        catch (InterruptedException ie) {
            fail(ie.getMessage());
        }
    }


    private void addTablesToPrefixIndex(List<TableTTLInfo> tableList, RowKeyMatcher rowKeyMatcher) {
        AtomicInteger tableId = new AtomicInteger(0);
        tableList.forEach(m -> {
            rowKeyMatcher.put(m.getMatchPattern(), tableId.incrementAndGet());
        });
    }

    private void addTablesToCache(List<TableTTLInfo> tableList, TableTTLInfoCache cache) {
        tableList.forEach(m -> {
            cache.addTable(m);
        });
    }

}