package org.apache.phoenix.end2end.prefix;


import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.prefix.search.PrefixIndex;
import org.apache.phoenix.prefix.table.TableTTLInfo;
import org.apache.phoenix.prefix.table.TableTTLInfoCache;
import org.apache.phoenix.util.ByteUtil;
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

public class PrefixIndexAndCacheTest {

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
	public void testSamplePrefixes() {
		PrefixIndex prefixIndex = new PrefixIndex();
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

		for (int i = 0; i < sampleTableList.size(); i++) {
			Integer tableId = cache.addTable(sampleTableList.get(i));
			prefixIndex.put(sampleTableList.get(i).getPrefix(), tableId);
			assertEquals(tableId.intValue(), i);
		}
		int offset = 0;
		for (int i = 0; i<sampleTableList.size(); i++) {
			assertEquals(prefixIndex.get(sampleTableList.get(i).getPrefix(), offset).intValue(), i);
		}

	}

	@Test
	public void testSingleSampleDataCount() {
		PrefixIndex prefixIndex = new PrefixIndex();
		TableTTLInfoCache cache = new TableTTLInfoCache();

		List<TableTTLInfo> sampleTableList = getSampleData();
		runTest(prefixIndex, cache, sampleTableList, 1, 1);

		//Assert results
		assertResults(prefixIndex, sampleTableList);
		assertResults(cache, sampleTableList);

	}

	@Test
	public void testRepeatingSampleDataCount() {		
		PrefixIndex prefixIndex = new PrefixIndex();
		TableTTLInfoCache cache = new TableTTLInfoCache();
		List<TableTTLInfo> sampleTableList = getSampleData();
		runTest(prefixIndex, cache, sampleTableList, 1, 25);
		
		//Assert results
		assertResults(prefixIndex, sampleTableList);
		assertResults(cache, sampleTableList);

	}
	
	@Test
	public void testConcurrentSampleDataCount() {
		PrefixIndex prefixIndex = new PrefixIndex();
		TableTTLInfoCache cache = new TableTTLInfoCache();
		List<TableTTLInfo> sampleTableList = getSampleData();
		runTest(prefixIndex, cache, sampleTableList, 5, 5);

		//Assert results
		assertResults(prefixIndex, sampleTableList);
		assertResults(cache, sampleTableList);
	}

	private void assertResults(PrefixIndex prefixIndex, List<TableTTLInfo> sampleTableList) {
		//Assert results
		int tableCountExpected = sampleTableList.size();
		int prefixCountActual = prefixIndex.getValidPrefixes();
		String message = String.format("expected = %d, actual = %d", tableCountExpected, prefixCountActual);
		assertTrue(message, tableCountExpected == prefixCountActual);
	}

	private void assertResults(TableTTLInfoCache cache, List<TableTTLInfo> sampleTableList) {
		//Assert results
		Set<TableTTLInfo> dedupedTables = new HashSet<TableTTLInfo>();
		dedupedTables.addAll(sampleTableList);

		int tableCountExpected = dedupedTables.size();
		int tableCountActual = cache.getNumTablesInCache();
		String message = String.format("expected = %d, actual = %d", tableCountExpected, tableCountActual);
		assertTrue(message, tableCountExpected == tableCountActual);
	}

	private void runTest(PrefixIndex targetPrefixIndex, TableTTLInfoCache cache,
			List<TableTTLInfo> sampleData, int numThreads, int numRepeats) {
		
		try {			
	        Thread[] threads = new Thread[numThreads];
	        final CountDownLatch latch = new CountDownLatch(threads.length);
	        for (int i = 0; i < threads.length; i++) {
	            threads[i] = new Thread(new Runnable() {
	                public void run() {
	                    try {                  
	                        for (int repeats = 0; repeats < numRepeats; repeats++) {
	                			addTablesToPrefixIndex(sampleData, targetPrefixIndex);
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
	
	
	private void addTablesToPrefixIndex(List<TableTTLInfo> tableList, PrefixIndex prefixIndex) {
		AtomicInteger tableId = new AtomicInteger(0);
		tableList.forEach(m -> {
			prefixIndex.put(m.getPrefix(), tableId.incrementAndGet());
		});
	}

	private void addTablesToCache(List<TableTTLInfo> tableList, TableTTLInfoCache cache) {
		tableList.forEach(m -> {
			cache.addTable(m);
		});
	}

}
