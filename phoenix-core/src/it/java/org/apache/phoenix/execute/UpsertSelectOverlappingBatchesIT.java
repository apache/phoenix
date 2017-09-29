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

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UpsertSelectOverlappingBatchesIT extends BaseUniqueNamesOwnClusterIT {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put("hbase.coprocessor.region.classes", SlowBatchRegionObserver.class.getName());
        serverProps.put("hbase.rowlock.wait.duration", "5000");
        serverProps.put(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, "100");
        Map<String,String> clientProps = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
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
    
	@Test
	public void testUpsertSelectSameBatchConcurrently() throws Exception {
		final String dataTable = generateUniqueName();
		final String index = "IDX_" + dataTable;
		// create the table and ensure its empty
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		Connection conn = driver.connect(url, props);
		conn.createStatement()
				.execute("CREATE TABLE " + dataTable + " (k INTEGER NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
		// create the index and ensure its empty as well
		conn.createStatement().execute("CREATE INDEX " + index + " ON " + dataTable + " (v1)");

		conn = DriverManager.getConnection(getUrl(), props);
		PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + dataTable + " VALUES(?,?,?)");
		conn.setAutoCommit(false);
		for (int i = 0; i < 100; i++) {
			stmt.setInt(1, i);
			stmt.setString(2, "v1" + i);
			stmt.setString(3, "v2" + i);
			stmt.execute();
		}
		conn.commit();

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
		conn.close();
	}
    
    public static class SlowBatchRegionObserver extends SimpleRegionObserver {
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
        	// model a slow batch that takes a long time
            if (miniBatchOp.size()==100) {
            	try {
					Thread.sleep(6000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
        }
    }
}

