/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.transactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import co.cask.tephra.TransactionManager;
import co.cask.tephra.TxConstants;
import co.cask.tephra.distributed.TransactionService;
import co.cask.tephra.metrics.TxMetricsCollector;
import co.cask.tephra.persist.InMemoryTransactionStateStorage;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.BeforeClass;
import org.junit.Test;

public class TransactionIT extends BaseHBaseManagedTimeIT {

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		config.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
//		config.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, ConnectionInfo.getZookeeperConnectionString(getUrl()));
		config.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
		config.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 1);

		ConnectionInfo connInfo = ConnectionInfo.create(getUrl());
	    ZKClientService zkClient = ZKClientServices.delegate(
	      ZKClients.reWatchOnExpire(
	        ZKClients.retryOnFailure(
	          ZKClientService.Builder.of(connInfo.getZookeeperConnectionString())
	            .setSessionTimeout(config.getInt(HConstants.ZK_SESSION_TIMEOUT,
	            		HConstants.DEFAULT_ZK_SESSION_TIMEOUT))
	            .build(),
	          RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
	        )
	      )
	    );
	    zkClient.startAndWait();

	    DiscoveryService discovery = new ZKDiscoveryService(zkClient);
	    final TransactionManager txManager = new TransactionManager(config, new InMemoryTransactionStateStorage(), new TxMetricsCollector());
	    TransactionService txService = new TransactionService(config, zkClient, discovery, txManager);
	    txService.startAndWait();
	}
	
	@Test
	public void testUpsert() throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY, k2 INTEGER) transactional=true";
		try {
			conn.setAutoCommit(false);
			conn.createStatement().execute(ddl);
			// upsert one row
			PreparedStatement stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
	        stmt.setInt(1, 1);
	        stmt.setInt(2, 1);
	        stmt.execute();
	        conn.commit();
	        // verify row exists
	        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t");
	        assertTrue(rs.next());
	        assertEquals(1,rs.getInt(1));
	        assertEquals(1,rs.getInt(1));
	        assertFalse(rs.next());
		}
        finally {
        	conn.close();
        }
	}
	
	@Test
	public void testColConflicts() throws Exception {
		Connection conn1 = DriverManager.getConnection(getUrl());
		Connection conn2 = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY, k2 INTEGER) transactional=true";
		try {
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			conn1.createStatement().execute(ddl);
			// upsert row using conn1
			PreparedStatement stmt = conn1.prepareStatement("UPSERT INTO t VALUES(?,?)");
	        stmt.setInt(1, 1);
	        stmt.setInt(2, 10);
	        stmt.execute();
	        // upsert row using conn2
 			stmt = conn2.prepareStatement("UPSERT INTO t VALUES(?,?)");
 	        stmt.setInt(1, 1);
 	        stmt.setInt(2, 11);
 	        stmt.execute();
 	        
 	        conn1.commit();
	        //second commit should fail
 	        try {
 	 	        conn2.commit();
 	 	        fail();
 	        }	
 	        catch (SQLException e) {
 	        	assertEquals(e.getErrorCode(), SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode());
 	        }
		}
        finally {
        	conn1.close();
        }
	}
	
	@Test
	public void testRowConflicts() throws Exception {
		Connection conn1 = DriverManager.getConnection(getUrl());
		Connection conn2 = DriverManager.getConnection(getUrl());
		String ddl = "CREATE TABLE t (k1 INTEGER PRIMARY KEY, k2 INTEGER, k3 INTEGER) transactional=true";
		try {
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);
			conn1.createStatement().execute(ddl);
			// upsert row using conn1
			PreparedStatement stmt = conn1.prepareStatement("UPSERT INTO t(k1,k2) VALUES(?,?)");
	        stmt.setInt(1, 1);
	        stmt.setInt(2, 10);
	        stmt.execute();
	        // upsert row using conn2
 			stmt = conn2.prepareStatement("UPSERT INTO t(k1,k3) VALUES(?,?)");
 	        stmt.setInt(1, 1);
 	        stmt.setInt(2, 11);
 	        stmt.execute();
 	        
 	        conn1.commit();
	        //second commit should fail
 	        try {
 	 	        conn2.commit();
 	 	        fail();
 	        }	
 	        catch (SQLException e) {
 	        	assertEquals(e.getErrorCode(), SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION.getErrorCode());
 	        }
		}
        finally {
        	conn1.close();
        }
	}


//	@AfterClass
//	public static void shutdownAfterClass() throws Exception {
//		txManager.stop();
//	}

}
