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
package org.apache.phoenix.execute;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class UpsertSelectOverlappingBatchesIT extends BaseTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UpsertSelectOverlappingBatchesIT.class);
    private Properties props;
    private static volatile String dataTable;
    private String index;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put("hbase.coprocessor.region.classes", SlowBatchRegionObserver.class.getName());
        serverProps.put("hbase.rowlock.wait.duration", "5000");
        serverProps.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "100");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()));
    }

    @AfterClass
    public static synchronized void tearDownClass() throws Exception {
        SlowBatchRegionObserver.SLOW_MUTATE = false;
        getUtility().shutdownMiniCluster();
    }

    private class UpsertSelectRunner implements Callable<Boolean> {
    	private final String dataTable;
    	private final int minIndex;
    	private final int maxIndex;
    	private final int numLoop;
    	
    	public UpsertSelectRunner (String dataTable, int minIndex, int maxIndex, int numLoop) {
    		this.dataTable = dataTable;
    		this.minIndex = minIndex;
    		this.maxIndex = maxIndex;
    		this.numLoop = numLoop;
    	}

		@Override
		public Boolean call() throws Exception {
			Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
			try (Connection myConn = DriverManager.getConnection(getUrl(), props)) {
				myConn.setAutoCommit(true);
				String time = String.valueOf(System.currentTimeMillis());
				String dml = "UPSERT INTO " + dataTable + " SELECT k, v1 || '" + time + "', v2 || '" + time
						+ "' FROM " + dataTable + " WHERE k >= " + minIndex + " AND k < " + maxIndex;
				myConn.setAutoCommit(true);
				for (int j = 0; j < numLoop; ++j) {
					myConn.createStatement().execute(dml);
				}
				return true;
			}
		}
    }

    private static class UpsertSelectLooper implements Runnable {
        private UpsertSelectRunner runner;
        public UpsertSelectLooper(UpsertSelectRunner runner) {
            this.runner = runner;
        }
        @Override
        public void run() {
            while (true) {
                try {
                    runner.call();
                }
                catch (Exception e) {
                    if (ExceptionUtils.indexOfThrowable(e, InterruptedException.class) != -1) {
                        LOGGER.info("Interrupted, exiting", e);
                        Thread.currentThread().interrupt();
                        return;
                    }
                    LOGGER.error("Hit exception while writing", e);
                }
            }
        }};

    @Before
    public void setup() throws Exception {
        SlowBatchRegionObserver.SLOW_MUTATE = false;
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
        dataTable = generateUniqueName();
        index = "IDX_" + dataTable;
        try (Connection conn = driver.connect(url, props)) {
            conn.createStatement().execute("CREATE TABLE " + dataTable
                    + " (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            // create the index and ensure its empty as well
            conn.createStatement().execute("CREATE INDEX " + index + " ON " + dataTable + " (v1)");
            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTable + " VALUES(?,?,?)");
            conn.setAutoCommit(false);
            for (int i = 0; i < 100; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "v1" + i);
                stmt.setString(3, "v2" + i);
                stmt.execute();
            }
            conn.commit();
        }
    }

	@Test
	public void testUpsertSelectSameBatchConcurrently() throws Exception {
		try (Connection conn = driver.connect(url, props)) {
		        int numUpsertSelectRunners = 5;
		        ExecutorService exec = Executors.newFixedThreadPool(numUpsertSelectRunners);
		        CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(exec);
		        List<Future<Boolean>> futures = Lists.newArrayListWithExpectedSize(numUpsertSelectRunners);
		        // run one UPSERT SELECT for 100 rows (that locks the rows for a long time)
		        futures.add(completionService.submit(new UpsertSelectRunner(dataTable, 0, 105, 1)));
		        // run four UPSERT SELECTS for 5 rows (that overlap with slow running UPSERT SELECT)
		        for (int i = 0; i < 100; i += 25) {
		            futures.add(completionService.submit(new UpsertSelectRunner(dataTable, i, i+25, 5)));
		        }
		        int received = 0;
		        while (received < futures.size()) {
		            Future<Boolean> resultFuture = completionService.take();
		            Boolean result = resultFuture.get();
		            received++;
		            assertTrue(result);
		        }
		        exec.shutdownNow();
		}
	}

    /**
     * Tests that splitting a region is not blocked indefinitely by UPSERT SELECT load
     */
	@Test
    public void testSplitDuringUpsertSelect() throws Exception {
        int numUpsertSelectRunners = 4;
        ExecutorService exec = Executors.newFixedThreadPool(numUpsertSelectRunners);
        try (Connection conn = driver.connect(url, props)) {
            final UpsertSelectRunner upsertSelectRunner =
                    new UpsertSelectRunner(dataTable, 0, 105, 1);
            // keep running slow upsert selects
            SlowBatchRegionObserver.SLOW_MUTATE = true;
            for (int i = 0; i < numUpsertSelectRunners; i++) {
                exec.submit(new UpsertSelectLooper(upsertSelectRunner));
                Thread.sleep(300);
            }

            // keep trying to split the region
            final HBaseTestingUtility utility = getUtility();
            final Admin admin = utility.getAdmin();
            final TableName dataTN = TableName.valueOf(dataTable);
            assertEquals(1, utility.getHBaseCluster().getRegions(dataTN).size());
            utility.waitFor(60000L, 1000, new Waiter.Predicate<Exception>() {
                @Override
                public boolean evaluate() throws Exception {
                    try {
                        List<RegionInfo> regions = admin.getRegions(dataTN);
                        if (regions.size() > 1) {
                            LOGGER.info("Found region was split");
                            return true;
                        }
                        if (regions.size() == 0) {
                            // This happens when region in transition or closed
                            LOGGER.info("No region returned");
                            return false;
                        }
                        ;
                        RegionInfo hRegion = regions.get(0);
                        LOGGER.info("Attempting to split region");
                        admin.splitRegionAsync(hRegion.getRegionName(), Bytes.toBytes(2));
                        return false;
                    } catch (NotServingRegionException | DoNotRetryRegionException re) {
                        // during split
                        return false;
                    } 
                }
            });
        } finally {
            SlowBatchRegionObserver.SLOW_MUTATE = false;
            exec.shutdownNow();
            exec.awaitTermination(60, TimeUnit.SECONDS);
        }
    }

    /**
     * Tests that UPSERT SELECT doesn't indefinitely block region closes
     */
    @Test
    public void testRegionCloseDuringUpsertSelect() throws Exception {
        int numUpsertSelectRunners = 4;
        ExecutorService exec = Executors.newFixedThreadPool(numUpsertSelectRunners);
        try (Connection conn = driver.connect(url, props)) {
            final UpsertSelectRunner upsertSelectRunner =
                    new UpsertSelectRunner(dataTable, 0, 105, 1);
            // keep running slow upsert selects
            SlowBatchRegionObserver.SLOW_MUTATE = true;
            for (int i = 0; i < numUpsertSelectRunners; i++) {
                exec.submit(new UpsertSelectLooper(upsertSelectRunner));
                Thread.sleep(300);
            }

            final HBaseTestingUtility utility = getUtility();
            // try to close the region while UPSERT SELECTs are happening,
            final HRegionServer dataRs = utility.getHBaseCluster().getRegionServer(0);
            final Admin admin = utility.getAdmin();
            final RegionInfo dataRegion =
                    admin.getRegions(TableName.valueOf(dataTable)).get(0);
            LOGGER.info("Closing data table region");
            admin.unassign(dataRegion.getEncodedNameAsBytes(), true);
            // make sure the region is offline
            utility.waitFor(60000L, 1000, new Waiter.Predicate<Exception>() {
                @Override
                public boolean evaluate() throws Exception {
                    List<RegionInfo> onlineRegions =
                            admin.getRegions(dataRs.getServerName());
                    for (RegionInfo onlineRegion : onlineRegions) {
                        if (onlineRegion.equals(dataRegion)) {
                            LOGGER.info("Data region still online");
                            return false;
                        }
                    }
                    LOGGER.info("Region is no longer online");
                    return true;
                }
            });
        } finally {
            SlowBatchRegionObserver.SLOW_MUTATE = false;
            exec.shutdownNow();
            exec.awaitTermination(60, TimeUnit.SECONDS);
        }
    }
    
    public static class SlowBatchRegionObserver extends SimpleRegionObserver {
        public static volatile boolean SLOW_MUTATE = false;
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
        	// model a slow batch that takes a long time
            if ((miniBatchOp.size()==100 || SLOW_MUTATE) && c.getEnvironment().getRegionInfo().getTable().getNameAsString().equals(dataTable)) {
            	try {
					Thread.sleep(6000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
        }
    }
}

